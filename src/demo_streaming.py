"""Measured restart experiment: producer stays active for 120 seconds while
the Silver query is stopped. Historical values are replayed on a documented
one-second test clock; source_event_time always preserves the original UTC time.
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Thread
import boto3
from pyspark.sql import functions as F
from common import session
from producer import publish
from streaming_pipeline import paths, start_ingestion, start_gold, stop

ROOT=Path(__file__).resolve().parents[1]


def run(run_id, pause_seconds=120):
    out=ROOT/'results/jalon2'/run_id; out.mkdir(parents=True,exist_ok=True)
    spark=session('Jalon2-Streaming-'+run_id)
    p=paths(run_id); all_queries={}; progress={}; sent=[]
    s3=boto3.client('s3',endpoint_url=os.environ['S3_ENDPOINT'],aws_access_key_id=os.environ['MINIO_ROOT_USER'],aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'])
    def record(label,**data):
        line=json.dumps(dict(phase=label,utc=datetime.now(timezone.utc).isoformat(),**data),default=str)
        with (out/'experiment.jsonl').open('a') as f: f.write(line+'\n')
        print('EXPERIMENT '+line,flush=True)
    def snapshot(qs):
        for name,q in qs.items():
            previous=progress.setdefault(name,{})
            for r in q.recentProgress: previous[str(r['batchId'])]=r
            if q.exception(): raise RuntimeError(str(q.exception()))
        (out/'progress.json').write_text(json.dumps(progress,indent=2,default=str))
    def drain(qs):
        for q in qs.values(): q.processAllAvailable()
        snapshot(qs)
    def checkpoint(label):
        prefix=p['cp'].replace('s3a://lake/','')+'/silver/offsets/'
        entries=[]
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket='lake',Prefix=prefix):
            for obj in page.get('Contents',[]):
                if obj['Key'].rsplit('/',1)[-1].isdigit():
                    entries.append({'key':obj['Key'],'content':s3.get_object(Bucket='lake',Key=obj['Key'])['Body'].read().decode()})
        (out/(label+'-offsets.json')).write_text(json.dumps(entries,indent=2))
        return entries
    raw=[json.loads(line) for line in (ROOT/'data/orders_history.jsonl').read_text().splitlines()]
    # Unique values from the real data, duplicates then injected explicitly.
    unique={}
    for row in raw: unique.setdefault((row['order_id'],row['status']),row)
    needed=600+pause_seconds*20+600
    seedrows=sorted(unique.values(),key=lambda r:(r['event_time'],r['order_id'],r['status']))[:needed]
    epoch=datetime(2026,11,14,12,0,tzinfo=timezone.utc)
    replay=[dict(row,event_time=(epoch+timedelta(seconds=i)).isoformat().replace('+00:00','Z'),scenario='test_clock_dataset_values') for i,row in enumerate(seedrows)]
    first=replay[:600]+[dict(row) for row in replay[100:120]]
    middle=replay[600:600+pause_seconds*20]
    last=replay[600+pause_seconds*20:]
    try:
        qs=start_ingestion(spark,run_id); all_queries.update(qs)
        rows,elapsed=publish(first,20,run_id=run_id);sent.extend(rows);drain(qs)
        before=spark.read.parquet(p['silver']).count()
        record('before_stop',silver_rows=before,produced=len(sent),seconds=elapsed)
        assert before==600, ('initial dedup failed',before)
        checkpoint('before')
        stop(qs); snapshot(qs)
        record('consumer_stopped',producer_rate=20,planned_seconds=pause_seconds)
        rows,elapsed=publish(middle,20,run_id=run_id);sent.extend(rows)
        record('producer_during_stop',produced=len(rows),seconds=elapsed)
        assert elapsed>=pause_seconds-0.1
        # Restart the same query/checkpoint: no startingOffsets reset.
        restart_started=time.monotonic()
        qs=start_ingestion(spark,run_id,console=False); all_queries.update(qs)
        drain(qs)
        recovered=spark.read.parquet(p['silver']).count()
        record('after_restart',silver_rows=recovered,recovery_seconds=time.monotonic()-restart_started)
        assert recovered==600+len(middle), ('recovery count',recovered)
        checkpoint('after')
        rows,elapsed=publish(last,20,run_id=run_id);sent.extend(rows);drain(qs)
        # Send a unique event 15 minutes behind the last observed event time.
        late=dict(last[-1],order_id='TEST-LATE-15MIN',event_time=(epoch+timedelta(seconds=needed-1-900)).isoformat().replace('+00:00','Z'),scenario='late_15min_probe')
        rows,_=publish([late],20,run_id=run_id);sent.extend(rows);drain(qs)
        # Two explicit control records advance watermarks and close old windows.
        controls=[dict(last[-1],order_id='TEST-WATERMARK-'+str(i),tenant_id='__control__',city='__control__',
            courier_id='SIM-CONTROL',amount_usd=0,source_subtotal_cents=0,status='CREATED',scenario='watermark_control',
            event_time=(epoch+timedelta(seconds=needed+1200+i*1200)).isoformat().replace('+00:00','Z')) for i in range(2)]
        for control in controls:
            rows,_=publish([control],20,run_id=run_id);sent.extend(rows);drain(qs)
        stop(qs);snapshot(qs)
        bronze=spark.read.json(p['bronze']); silver=spark.read.parquet(p['silver'])
        keys={(r['order_id'],r['status']) for r in silver.select('order_id','status').collect()}
        expected={(r['order_id'],r['status']) for r in sent if r['scenario']!='late_15min_probe'}
        assert keys==expected, {'missing':len(expected-keys),'extra':len(keys-expected)}
        assert silver.count()==len(keys)
        assert bronze.count()==len(sent)
        late_kept=silver.filter(F.col('scenario')=='late_15min_probe').count()
        late_raw=bronze.filter(F.get_json_object('raw_json','$.scenario')=='late_15min_probe').count()
        assert late_kept==0 and late_raw==1
        gold_q=start_gold(spark,run_id);all_queries.update(gold_q);drain(gold_q);drain(gold_q);stop(gold_q);snapshot(gold_q)
        gold=spark.read.format('delta').load(p['gold'])
        history=spark.sql("DESCRIBE HISTORY delta.`"+p['gold']+"`")
        history.show(truncate=False)
        gold.filter(F.col('tenant_id')!='__control__').orderBy('city','window').show(30,truncate=False)
        (out/'delta-history.json').write_text(json.dumps([r.asDict(recursive=True) for r in history.collect()],indent=2,default=str))
        business=silver.filter(F.col('tenant_id')!='__control__')
        oracle=business.groupBy('tenant_id','city',F.window('event_time','10 minutes')).agg(
            F.sum(F.when(F.col('status')=='CREATED',1).otherwise(0)).alias('orders_created'),
            F.sum(F.when(F.col('status')=='DELIVERED',1).otherwise(0)).alias('orders_delivered'),
            F.sum(F.when(F.col('status')=='DELIVERED',F.col('amount_usd')).otherwise(F.lit(0))).alias('revenue_usd'))
        actual=gold.filter(F.col('tenant_id')!='__control__').select(oracle.columns)
        assert oracle.exceptAll(actual).count()==0 and actual.exceptAll(oracle).count()==0,'Gold differs from batch oracle'
        results={'run_id':run_id,'status':'PASS','dataset_events':needed,'sent_including_tests':len(sent),
            'bronze_rows':bronze.count(),'silver_rows':silver.count(),'injected_duplicates':20,
            'late_probe_in_bronze':late_raw,'late_probe_in_silver':late_kept,'before_stop':before,'after_restart':recovered,
            'producer_pause_test_seconds':pause_seconds,'no_loss_or_duplicates_on_recovery':True,
            'gold_matches_batch_oracle':True,'gold_rows':actual.count(),'delta_versions':history.count(),
            'paths':p,'application_id':spark.sparkContext.applicationId,
            'currency':'USD','replay_clock':'one second per source event; original timestamp retained in source_event_time',
            'sliding_rows':spark.read.format('delta').load(p['sliding']).count(),
            'session_rows':spark.read.format('delta').load(p['sessions']).count()}
        for name in ['gold','sliding','sessions']:
            data=spark.read.format('delta').load(p[name])
            (out/(name+'-sample.json')).write_text(json.dumps([r.asDict(recursive=True) for r in data.limit(50).collect()],indent=2,default=str))
        (out/'validation.json').write_text(json.dumps(results,indent=2))
        (ROOT/'results/jalon2/latest.json').write_text(json.dumps(results,indent=2))
        (out/'published-events.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in sent))
        checkpoint('final');record('validation_passed',**results)
    finally:
        stop(all_queries)
        spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('run-%Y%m%d-%H%M%S'))
    parser.add_argument('--pause-seconds',type=int,default=120)
    args=parser.parse_args();run(args.run_id,args.pause_seconds)

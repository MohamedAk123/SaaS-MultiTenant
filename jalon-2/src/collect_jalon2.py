import json,os,re
from pathlib import Path
import boto3
from kafka import KafkaConsumer,TopicPartition
from common import ROOT

out=ROOT/'results/jalon2';out.mkdir(parents=True,exist_ok=True)
latest=json.loads((out/'latest.json').read_text())
run_dir=out/latest['run_id']
progress=json.loads((run_dir/'progress.json').read_text())
compact={}
for name,batches in progress.items():
    compact[name]=[{k:r.get(k) for k in ['batchId','timestamp','numInputRows','inputRowsPerSecond','processedRowsPerSecond','durationMs','eventTime','stateOperators','sources']} for r in batches.values()]
(out/'progress-proof.json').write_text(json.dumps(compact,indent=2))
for name in ['before-offsets','after-offsets','final-offsets','delta-history','gold-sample','sliding-sample','sessions-sample']:
    (out/(name+'.json')).write_bytes((run_dir/(name+'.json')).read_bytes())
(out/'restart-experiment.jsonl').write_bytes((run_dir/'experiment.jsonl').read_bytes())
summary={}
for name,rows in compact.items():
    nonempty=[r for r in rows if r['numInputRows']]
    summary[name]={'batches':len(rows),'nonempty_batches':len(nonempty),
        'max_trigger_ms':max((r.get('durationMs',{}).get('triggerExecution',0) for r in rows),default=0),
        'rows_dropped_by_watermark':sum(s.get('numRowsDroppedByWatermark',0) for r in rows for s in (r.get('stateOperators') or [])),
        'duplicates_dropped':sum(s.get('customMetrics',{}).get('numDroppedDuplicateRows',0) for r in rows for s in (r.get('stateOperators') or []))}
(out/'streaming-metrics.json').write_text(json.dumps(summary,indent=2))
consumer=KafkaConsumer(bootstrap_servers='kafka:19092',enable_auto_commit=False)
offsets=consumer.end_offsets([TopicPartition('orders',i) for i in range(3)])
(out/'kafka-end-offsets.json').write_text(json.dumps({str(p.partition):n for p,n in offsets.items()},indent=2));consumer.close()
s3=boto3.client('s3',endpoint_url=os.environ['S3_ENDPOINT'],aws_access_key_id=os.environ['MINIO_ROOT_USER'],aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'])
inventory=[]
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='lake'):
    for obj in page.get('Contents',[]): inventory.append({'key':obj['Key'],'bytes':obj['Size']})
(out/'lake-inventory.json').write_text(json.dumps(inventory,indent=2))
(ROOT/'evidence/lake-jalon2.txt').write_text('\n'.join(f"{r['bytes']:>10} lake/{r['key']}" for r in inventory))
log=(ROOT/'results/streaming-demo.log').read_bytes()
text=log.decode('utf-16' if log.startswith((b'\xff\xfe',b'\xfe\xff')) else 'utf-8',errors='replace')
text=re.sub(r'\x1b\[[0-9;]*[A-Za-z]','',text)
lines=[l for l in text.splitlines() if l.startswith(('+','|','Batch:','EXPERIMENT ','---'))]
(ROOT/'evidence/streaming-console.txt').write_text('\n'.join(lines),encoding='utf-8')
# Contract for a Next.js dashboard: partition by tenant and serve only the
# authenticated tenant in a real SaaS. This offline JSON is demonstration data.
graph=json.loads((ROOT/'results/graphframes.json').read_text())
dashboard={'dataset':json.loads((ROOT/'data/generation_manifest.json').read_text()),
    'streaming':latest,'metrics':summary,'graph':{k:v for k,v in graph.items() if k!='anomalies'},
    'kpi_sample':json.loads((out/'gold-sample.json').read_text()),
    'graph_validation':json.loads((ROOT/'results/graph_validation.json').read_text())}
(out/'dashboard-data.json').write_text(json.dumps(dashboard,indent=2))
print(json.dumps({'status':'PASS','streaming':summary,'lake_objects':len(inventory)},indent=2))

"""Interactive pipeline. Run producer.py --mode kafka --run-id SAME_ID separately."""
import argparse,time,json
from common import session,ROOT
from streaming_pipeline import start_ingestion,start_gold,paths,stop

parser=argparse.ArgumentParser();parser.add_argument('--run-id',default='manual')
args=parser.parse_args();spark=session('Jalon2-Live-'+args.run_id)
queries=start_ingestion(spark,args.run_id);gold_started=False
try:
    while True:
        for q in queries.values():
            if q.exception(): raise RuntimeError(str(q.exception()))
        if not gold_started:
            try:
                spark.read.parquet(paths(args.run_id)['silver']).schema
                queries.update(start_gold(spark,args.run_id));gold_started=True
            except Exception as exc:
                if 'PATH_NOT_FOUND' not in str(exc) and 'UNABLE_TO_INFER_SCHEMA' not in str(exc): raise
        time.sleep(5)
except KeyboardInterrupt:
    pass
finally:
    stop(queries);spark.stop()

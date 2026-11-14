"""Replay the published Kaggle dataset; --mode file builds the adapter outputs."""
import argparse
import json
import time
from pathlib import Path
from kafka import KafkaProducer
from prepare_dataset import prepare

ROOT=Path(__file__).resolve().parents[1]


def publish(rows, rate=20, topic='orders', run_id='demo'):
    p=KafkaProducer(bootstrap_servers='kafka:19092',acks='all',retries=5,
        key_serializer=lambda x:x.encode(),value_serializer=lambda x:json.dumps(x).encode())
    start=time.monotonic()
    sent=[]
    try:
        for i,row in enumerate(rows):
            event=dict(row,run_id=run_id,scenario=row.get('scenario','dataset_replay'))
            event['order_id']=run_id+':'+row['order_id']
            p.send(topic,key=event['restaurant_id'],value=event).get(timeout=30)
            sent.append(event)
            if rate:
                time.sleep(max(0,(i+1)/rate-(time.monotonic()-start)))
        p.flush()
    finally:
        p.close()
    return sent, time.monotonic()-start


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--mode',choices=['file','kafka'],default='file')
    parser.add_argument('--n',type=int,default=50000)
    parser.add_argument('--rate',type=float,default=20)
    parser.add_argument('--run-id',default='manual')
    parser.add_argument('--topic',default='orders')
    a=parser.parse_args()
    if a.mode=='file':
        if a.n!=50000: parser.error('The reproducible batch contains exactly 50000 events.')
        prepare()
    else:
        if not (ROOT/'data/orders_history.jsonl').exists(): prepare()
        rows=[json.loads(line) for line in (ROOT/'data/orders_history.jsonl').read_text().splitlines()][:a.n]
        sent,elapsed=publish(rows,a.rate,a.topic,a.run_id)
        print(json.dumps({'sent':len(sent),'elapsed_seconds':elapsed,'events_per_second':len(sent)/elapsed,'run_id':a.run_id}))


if __name__=='__main__': main()

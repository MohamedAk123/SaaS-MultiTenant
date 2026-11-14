"""Kaggle DoorDash v1 -> 50,000 replay events; provenance is explicit.

Only CREATED and DELIVERED timestamps are observed. No cancellation is invented.
Cities are anonymised market labels, USD is retained, distance is unavailable.
Customer/courier IDs and tenant access roles are demonstration fixtures.
"""
import argparse
import csv
import hashlib
import json
import random
import urllib.request
import zipfile
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URL = 'https://www.kaggle.com/api/v1/datasets/download/dharun4772/doordash-eta-prediction?datasetVersionNumber=1'
PAGE = 'https://www.kaggle.com/datasets/dharun4772/doordash-eta-prediction'
SOURCE_SHA256 = 'b87ae94bd8bf99accc8fd1873dde1ef5f5e1ce76074f22abdb734c54a1bc8d94'


def iso(dt):
    return dt.isoformat(timespec='seconds').replace('+00:00', 'Z')


def prepare():
    raw = ROOT/'data/external/doordash'
    raw.mkdir(parents=True, exist_ok=True)
    archive = raw/'source.zip'
    if not archive.exists():
        urllib.request.urlretrieve(URL, archive)
    path = raw/'historical_data.csv'
    if not path.exists():
        with zipfile.ZipFile(archive) as z:
            member = next(n for n in z.namelist() if Path(n).name == path.name)
            path.write_bytes(z.read(member))
    if hashlib.sha256(path.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError('Source SHA-256 differs from the verified Kaggle v1 file. Do not silently reuse changed data.')
    rows, rejected = [], Counter()
    with path.open(encoding='utf-8-sig', newline='') as f:
        for line, r in enumerate(csv.DictReader(f), start=2):
            try:
                start = datetime.strptime(r['created_at'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                end = datetime.strptime(r['actual_delivery_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                amount, items = int(r['subtotal']), int(r['total_items'])
                if not r['market_id'] or not r['store_id'] or amount < 0 or items <= 0 or end < start:
                    raise ValueError('invalid required fields')
                # Exclude extreme/noisy journeys longer than one day, explicitly counted.
                if (end-start).total_seconds() > 86400:
                    rejected['delivery_over_24h'] += 1
                    continue
                rows.append((line, r, start, end, amount, items))
            except (ValueError, KeyError):
                rejected['missing_or_invalid_required_fields'] += 1
    # Stable restaurant key includes market: no cross-market ID collisions.
    counts = Counter((r['market_id'], r['store_id']) for _,r,*_ in rows)
    selected = {key for key,_ in sorted(counts.items(), key=lambda x:(-x[1],x[0]))[:200]}
    pool = sorted((x for x in rows if (x[1]['market_id'],x[1]['store_id']) in selected),key=lambda x:(x[2],x[0]))
    sample = pool[:24500]
    assert len(sample) == 24500
    reference = []
    for market, store in sorted(selected):
        categories=Counter(r['store_primary_category'] or 'unknown' for _,r,*_ in rows
                           if r['market_id']==market and r['store_id']==store)
        reference.append({'restaurant_id':f'DD-{market}-{store}', 'name':f'Anonymised DoorDash store {store}',
            'city':f'Market-{market}', 'category':categories.most_common(1)[0][0],
            'latitude':None, 'longitude':None})
    events=[]
    for line,r,start,end,amount,items in sample:
        base={'order_id':f'DD-row-{line:07}', 'restaurant_id':f"DD-{r['market_id']}-{r['store_id']}",
              'customer_id':f'SIM-C-{line:07}', 'courier_id':f"SIM-L-{r['market_id']}-{line%12:02}",
              'tenant_id':f"tenant-{r['market_id']}", 'city':f"Market-{r['market_id']}",
              'amount_usd':amount/100, 'source_subtotal_cents':amount, 'items_count':items,
              'distance_km':None, 'source_row':line, 'source_dataset':'kaggle-doordash-v1',
              'simulated_fields':'customer_id,courier_id,tenant_id,replay_arrival_time'}
        for status,t in [('CREATED',start),('DELIVERED',end)]:
            events.append(dict(base,status=status,event_time=iso(t),source_event_time=iso(t)))
    rng=random.Random(2026)
    delayed=set(rng.sample(range(len(events)),2450))
    arrivals=[]
    for i,event in enumerate(events):
        dt=datetime.fromisoformat(event['event_time'].replace('Z','+00:00'))
        delay=rng.randint(1,900) if i in delayed else 0
        arrivals.append((dt+timedelta(seconds=delay),i,event))
    for i in rng.sample(range(len(events)),1000):
        arr=arrivals[i]
        arrivals.append((arr[0]+timedelta(seconds=1),i,dict(arr[2])))
    arrivals.sort(key=lambda x:(x[0],x[1]))
    out=ROOT/'data/orders_history.jsonl'
    out.write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for _,_,r in arrivals),encoding='utf-8')
    ref=ROOT/'data/restaurants.json'
    ref.write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in reference),encoding='utf-8')
    manifest={'source':PAGE,'dataset_ref':'dharun4772/doordash-eta-prediction','version':1,
      'license_as_declared_by_kaggle':'MIT','source_note':'DoorDash deliveries early 2015; noise added by source to obfuscate business details.',
      'source_rows':197428,'valid_source_rows':len(rows),'excluded':dict(rejected),'selection':'200 most frequent market/store pairs, first 24500 complete orders chronologically',
      'seed':2026,'events':50000,'restaurants':200,'unique_order_status':49000,'duplicate_rows':1000,
      'late_original_events':2450,'late_original_ratio':0.05,'max_simulated_delay_seconds':900,
      'currency':'USD','city_note':'Market identifiers, not real city names; no Moroccan geography inferred.',
      'missing_fields':['distance_km','latitude','longitude','real_customer_id','real_courier_id','PICKED_UP','CANCELLED'],
      'simulated_fields':['customer_id','courier_id','tenant_id','arrival_delay','duplicates','graph_access_policy'],
      'sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [path,out,ref]}}
    (ROOT/'data/generation_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps(manifest,indent=2))
    return manifest


if __name__=='__main__':
    prepare()

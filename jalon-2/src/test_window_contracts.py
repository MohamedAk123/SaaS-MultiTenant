"""Small independent examples for cancellation and the 5-minute session gap.
These explicit fixtures are not included in reported dataset KPI.
"""
from datetime import datetime
from pyspark.sql import functions as F
from common import session,save

spark=session('Jalon2-Tests-Fenetres')
try:
    rows=[('T','City','L','DELIVERED',100.0,datetime(2026,1,1,12,1)),
          ('T','City','L','CANCELLED',40.0,datetime(2026,1,1,12,2)),
          ('T','City','L','DELIVERED',20.0,datetime(2026,1,1,12,8))]
    df=spark.createDataFrame(rows,['tenant_id','city','courier_id','status','amount_usd','event_time'])
    t=df.groupBy(F.window('event_time','10 minutes')).agg(
        F.sum(F.when(F.col('status')=='DELIVERED',F.col('amount_usd')).otherwise(0)).alias('revenue')).first()
    assert t.revenue==120
    sliding=df.groupBy(F.window('event_time','10 minutes','5 minutes')).agg(
        F.count('*').alias('n'),F.sum(F.when(F.col('status')=='CANCELLED',1).otherwise(0)).alias('cancel'))
    observed={r.window.start.strftime('%H:%M'):(r.n,r.cancel) for r in sliding.collect()}
    assert observed=={'11:55':(2,1),'12:00':(3,1),'12:05':(1,0)},observed
    sessions=df.groupBy('courier_id',F.session_window('event_time','5 minutes')).count().collect()
    assert sorted(r['count'] for r in sessions)==[1,2]
    save('window_contract_tests.json',{'status':'PASS','source':'explicit synthetic unit fixtures',
        'tumbling_revenue_usd':120,'sliding_expected':observed,'sessions':2,
        'cancellation_definition':'CANCELLED / (DELIVERED + CANCELLED), by terminal-event time'})
    print('Window contracts PASS')
finally: spark.stop()

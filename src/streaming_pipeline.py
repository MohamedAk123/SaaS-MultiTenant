"""Parties 4/5: independent checkpoints; native file/Delta streaming sinks.
Silver deduplicates Kafka, then Gold reads the committed Silver file log.
"""
from pyspark.sql import functions as F
from common import ORDER_SCHEMA, RESTAURANTS, RESTAURANT_SCHEMA


def paths(run_id):
    return {'bronze':f's3a://lake/bronze/orders_raw/{run_id}',
            'silver':f's3a://lake/silver/orders_clean/{run_id}',
            'gold':f's3a://lake/gold/kpi_city_10min/{run_id}',
            'sliding':f's3a://lake/gold/cancellation_sliding/{run_id}',
            'sessions':f's3a://lake/gold/courier_sessions/{run_id}',
            'cp':f's3a://lake/checkpoints/{run_id}'}


def kafka_source(spark,topic='orders',max_offsets=500):
    return (spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:19092')
        .option('subscribe',topic).option('startingOffsets','earliest')
        .option('maxOffsetsPerTrigger',max_offsets).option('failOnDataLoss','true')
        .option('groupIdPrefix','jalon2').load())


def parsed(source,run_id):
    return source.select(F.from_json(F.col('value').cast('string'),ORDER_SCHEMA).alias('e'))\
        .select('e.*').filter(F.col('run_id')==run_id)


def clean_stream(events,spark):
    valid=events.filter(F.col('event_time').isNotNull() & F.col('order_id').isNotNull()
        & F.col('status').isin('CREATED','PICKED_UP','DELIVERED','CANCELLED')
        & (F.col('amount_usd')>=0) & (F.col('items_count')>0))
    # Requested API: state is not evicted when event_time is absent from the key.
    dedup=valid.withWatermark('event_time','10 minutes').dropDuplicates(['order_id','status'])
    dim=spark.read.schema(RESTAURANT_SCHEMA).json(RESTAURANTS)\
        .select('restaurant_id',F.col('name').alias('restaurant_name'),'category')
    return (dedup.join(F.broadcast(dim),'restaurant_id','inner')
        .withColumn('distance_band',F.when(F.col('distance_km').isNull(),'unknown')
             .when(F.col('distance_km')<2,'court').when(F.col('distance_km')<=5,'moyen').otherwise('long'))
        .withColumn('date',F.to_date('event_time')))


def start_ingestion(spark,run_id,topic='orders',console=True):
    p=paths(run_id); source=kafka_source(spark,topic); events=parsed(source,run_id)
    raw=source.filter(F.get_json_object(F.col('value').cast('string'),'$.run_id')==run_id)
    raw=raw.select(F.col('value').cast('string').alias('raw_json'),'topic','partition','offset','timestamp')
    queries={}
    queries['bronze']=(raw.writeStream.format('json').outputMode('append')
        .option('path',p['bronze']).option('checkpointLocation',p['cp']+'/bronze')
        .trigger(processingTime='5 seconds').queryName('bronze_'+run_id).start())
    clean=clean_stream(events,spark)
    queries['silver']=(clean.writeStream.format('parquet').outputMode('append').partitionBy('city','date')
        .option('path',p['silver']).option('checkpointLocation',p['cp']+'/silver')
        .trigger(processingTime='5 seconds').queryName('silver_'+run_id).start())
    if console:
        queries['raw_console']=(events.writeStream.format('console').outputMode('append')
            .option('truncate',True).option('numRows',5).option('checkpointLocation',p['cp']+'/raw_console')
            .trigger(processingTime='10 seconds').queryName('raw_console_'+run_id).start())
        queries['counts_console']=(events.groupBy('city','status').count().writeStream.format('console')
            .outputMode('complete').option('numRows',20).option('truncate',False)
            .option('checkpointLocation',p['cp']+'/counts_console').trigger(processingTime='10 seconds')
            .queryName('counts_console_'+run_id).start())
    return queries


def start_gold(spark,run_id):
    p=paths(run_id)
    schema=spark.read.parquet(p['silver']).schema
    source=spark.readStream.schema(schema).parquet(p['silver']).withWatermark('event_time','10 minutes')
    keys=['tenant_id','city']
    tumbling=source.groupBy(*keys,F.window('event_time','10 minutes')).agg(
        F.sum(F.when(F.col('status')=='CREATED',1).otherwise(0)).alias('orders_created'),
        F.sum(F.when(F.col('status')=='DELIVERED',1).otherwise(0)).alias('orders_delivered'),
        F.sum(F.when(F.col('status')=='DELIVERED',F.col('amount_usd')).otherwise(F.lit(0))).alias('revenue_usd'))
    sliding=(source.filter(F.col('status').isin('DELIVERED','CANCELLED'))
        .groupBy(*keys,F.window('event_time','10 minutes','5 minutes'))
        .agg(F.count('*').alias('terminal_orders'),
             F.sum(F.when(F.col('status')=='CANCELLED',1).otherwise(0)).alias('cancelled_orders'))
        .withColumn('cancellation_rate',F.col('cancelled_orders')/F.col('terminal_orders')))
    sessions=(source.filter(F.col('courier_id').isNotNull())
        .groupBy('tenant_id','courier_id',F.session_window('event_time','5 minutes'))
        .agg(F.count('*').alias('events'),F.min('event_time').alias('first_event'),F.max('event_time').alias('last_event'))
        .withColumn('observed_duration_seconds',F.col('last_event').cast('long')-F.col('first_event').cast('long')))
    queries={}
    for name,df in [('gold',tumbling),('sliding',sliding),('sessions',sessions)]:
        queries[name]=(df.writeStream.format('delta').outputMode('append').option('path',p[name])
            .option('checkpointLocation',p['cp']+'/'+name).trigger(processingTime='5 seconds')
            .queryName(name+'_'+run_id).start())
    return queries


def stop(queries):
    for q in queries.values():
        if q.isActive: q.stop()

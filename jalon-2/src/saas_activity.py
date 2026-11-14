"""Persist live SaaS activities in Delta and publish a display snapshot."""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from common import RESULTS, session
from streaming_pipeline import kafka_source

CHECKPOINT = 's3a://lake/checkpoints/saas-activity-v1'
SILVER = 's3a://lake/silver/saas_activity/'
SCHEMA = (StructType().add('event_id', 'string').add('tenant_id', 'string')
          .add('kind', 'string').add('resource', 'string').add('event_time', 'timestamp'))


def ensure_bucket():
    storage = boto3.client('s3', endpoint_url='http://minio:9000',
                           aws_access_key_id=os.environ['MINIO_ROOT_USER'],
                           aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'])
    if not any(bucket['Name'] == 'lake' for bucket in storage.list_buckets()['Buckets']):
        storage.create_bucket(Bucket='lake')


def main():
    ensure_bucket()
    consumer = KafkaConsumer(bootstrap_servers='kafka:19092', enable_auto_commit=False,
                             request_timeout_ms=10000, session_timeout_ms=5000)
    try:
        for _ in range(30):
            if 'saas-events' in consumer.topics():
                break
            time.sleep(2)
        else:
            raise RuntimeError('The outbox did not create saas-events.')
    finally:
        consumer.close()
    spark = session('SaaS-Activity-Next.js')
    events = (kafka_source(spark, 'saas-events')
              .select(F.from_json(F.col('value').cast('string'), SCHEMA).alias('e')).select('e.*')
              .filter(F.col('tenant_id').rlike('^tenant-[1-6]$') & F.col('event_id').isNotNull()
                      & F.col('event_time').isNotNull() & F.col('kind').isin('ACTION', 'ERROR', 'ACCESS'))
              .withWatermark('event_time', '10 minutes').dropDuplicatesWithinWatermark(['event_id']))

    def project(batch, epoch):
        batch.persist()
        try:
            if batch.isEmpty():
                return
            (batch.write.format('delta').mode('append').option('txnAppId', 'saas-activity-v1')
             .option('txnVersion', epoch).partitionBy('tenant_id').save(SILVER))
        finally:
            batch.unpersist()

        activity = spark.read.format('delta').load(SILVER)
        view = {}

        def tenant_view(tenant):
            return view.setdefault(tenant, {'counts': {}, 'windows': [], 'events': [],
                                            'updatedAt': datetime.now(timezone.utc).isoformat()})

        for row in activity.groupBy('tenant_id', 'kind').count().collect():
            tenant_view(row.tenant_id)['counts'][row.kind] = row['count']
        windowed = (activity.groupBy('tenant_id', F.window('event_time', '10 minutes').alias('period'))
                    .agg(F.count('*').alias('total'), F.sum((F.col('kind') == 'ERROR').cast('int')).alias('errors')))
        history = Window.partitionBy('tenant_id').orderBy(F.col('period.start').desc())
        for row in windowed.withColumn('rank', F.row_number().over(history)).where('rank <= 12').collect():
            tenant_view(row.tenant_id)['windows'].append({'start': str(row.period.start), 'total': row.total, 'errors': row.errors})
        latest = Window.partitionBy('tenant_id').orderBy(F.col('event_time').desc(), 'event_id')
        for row in activity.withColumn('rank', F.row_number().over(latest)).where('rank <= 30').collect():
            tenant_view(row.tenant_id)['events'].append({'id': row.event_id, 'kind': row.kind, 'resource': row.resource,
                                                       'eventTime': row.event_time.isoformat() + 'Z'})
        target = RESULTS / 'live-dashboard.json'
        temporary = target.with_suffix('.tmp')
        temporary.write_text(json.dumps(view), encoding='utf-8')
        temporary.replace(target)

    query = (events.writeStream.foreachBatch(project).option('checkpointLocation', CHECKPOINT)
             .trigger(processingTime='5 seconds').queryName('saas_activity_dashboard').start())
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()

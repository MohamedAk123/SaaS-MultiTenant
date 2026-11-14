import json
import os
from decimal import Decimal
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                              DoubleType, DecimalType, TimestampType)

ROOT = Path(os.environ.get("PROJECT_ROOT", "/workspace"))
RESULTS = ROOT / "results"
HISTORY = "s3a://lake/bronze/history/orders_history.jsonl"
RESTAURANTS = "s3a://lake/bronze/reference/restaurants.json"
SILVER = "s3a://lake/silver/orders_history/"

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()), StructField("restaurant_id", StringType()),
    StructField("customer_id", StringType()), StructField("city", StringType()),
    StructField("status", StringType()), StructField("amount_usd", DecimalType(12, 2)),
    StructField("items_count", IntegerType()), StructField("distance_km", DoubleType()),
    StructField("courier_id", StringType()), StructField("event_time", TimestampType()),
    StructField("tenant_id", StringType()), StructField("source_event_time", StringType()),
    StructField("source_row", IntegerType()), StructField("source_dataset", StringType()),
    StructField("source_subtotal_cents", IntegerType()), StructField("simulated_fields", StringType()),
    StructField("run_id", StringType()), StructField("scenario", StringType()),
])
RESTAURANT_SCHEMA = StructType([
    StructField("restaurant_id", StringType()), StructField("name", StringType()),
    StructField("city", StringType()), StructField("category", StringType()),
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
])


def session(name):
    spark = (SparkSession.builder.appName(name)
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "jupyter"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.memory", "768m")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", os.environ.get("SPARK_MAX_CORES", "4"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", "4")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/events")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    RESULTS.mkdir(parents=True, exist_ok=True)
    return spark


def save(name, data):
    (RESULTS / name).write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def parse_event(line):
    return json.loads(line, parse_float=Decimal)


def cents(row):
    return int(row["amount_usd"] * 100)


def add_pair(a, b):
    return a[0] + b[0], a[1] + b[1]


def distance_band(value):
    if value is None:
        return None
    if value < 2:
        return "court"
    return "moyen" if value <= 5 else "long"

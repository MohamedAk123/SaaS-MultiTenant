"""Create project resources and copy the generated input files into Bronze."""
import json
import os
from pathlib import Path
import boto3
from botocore.config import Config
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

root = Path("/workspace")
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    region_name="us-east-1", config=Config(signature_version="s3v4"))
if "lake" not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket="lake")
for filename, key in [
    ("orders_history.jsonl", "bronze/history/orders_history.jsonl"),
    ("restaurants.json", "bronze/reference/restaurants.json"),
    ("generation_manifest.json", "bronze/history/generation_manifest.json"),
]:
    s3.upload_file(str(root / "data" / filename), "lake", key)
admin = KafkaAdminClient(bootstrap_servers="kafka:19092", client_id="jalon1-bootstrap")
try:
    admin.create_topics([NewTopic("orders", num_partitions=3, replication_factor=1)])
except TopicAlreadyExistsError:
    pass
topic = admin.describe_topics(["orders"])[0]
assert len(topic["partitions"]) == 3, "orders must have exactly 3 partitions"
admin.close()
evidence = {"bucket": "lake", "topic": "orders", "partitions": len(topic["partitions"]),
            "objects": [{"key": x["Key"], "bytes": x["Size"]} for x in s3.list_objects_v2(Bucket="lake")["Contents"]]}
(root / "results" / "bootstrap.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
print(json.dumps(evidence, indent=2))

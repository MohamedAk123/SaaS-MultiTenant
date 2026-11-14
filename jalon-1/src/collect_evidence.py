"""Collect measured stage/job metadata, storage inventory and runtime versions."""
import importlib.metadata
import json
import os
import platform
import urllib.request
from pathlib import Path
import boto3
from botocore.config import Config

root = Path("/workspace")
results = root / "results"
evidence = root / "evidence"
evidence.mkdir(exist_ok=True)
summaries = {}
for part, filename in [("part1", "part1_cluster.json"), ("part2", "part2_rdd.json"), ("part3", "part3_sql.json")]:
    app_id = json.loads((results / filename).read_text(encoding="utf-8"))["application_id"]
    event_path = Path("/opt/spark/events") / app_id
    jobs, stages = [], []
    for line in event_path.read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        if event["Event"] == "SparkListenerJobStart":
            jobs.append({"job_id": event["Job ID"], "stage_ids": event["Stage IDs"],
                         "group": event.get("Properties", {}).get("spark.jobGroup.id"),
                         "description": event.get("Properties", {}).get("spark.job.description")})
        elif event["Event"] == "SparkListenerStageCompleted":
            s = event["Stage Info"]
            stages.append({"stage_id": s["Stage ID"], "attempt": s["Stage Attempt ID"],
                "name": s["Stage Name"], "tasks": s["Number of Tasks"], "parents": s["Parent IDs"],
                "failure": s.get("Failure Reason"),
                "duration_ms": s.get("Completion Time", 0) - s.get("Submission Time", 0)})
    assert all(not s["failure"] for s in stages), "A stage failed; inspect the logs"
    summaries[part] = {"application_id": app_id, "jobs": jobs, "completed_stages": stages,
        "number_of_jobs": len(jobs), "number_of_completed_stage_attempts": len(stages),
        "history_url": "http://localhost:18080/history/" + app_id + "/jobs/"}
(results / "execution_metrics.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"], aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    config=Config(signature_version="s3v4"))
objects = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket="lake"):
    objects.extend({"key": x["Key"], "bytes": x["Size"]} for x in page.get("Contents", []))
(results / "minio_inventory.json").write_text(json.dumps(objects, indent=2), encoding="utf-8")
(evidence / "minio_inventory.txt").write_text("\n".join(f'{o["bytes"]:>10}  lake/{o["key"]}' for o in objects), encoding="utf-8")
assert sum(o["key"].endswith(".parquet") for o in objects) > 0
versions = {"python": platform.python_version(), "spark": "3.5.6", "scala": "2.12",
    "hadoop": "3.3.4", "kafka": "3.9.1", "minio_source_tag": "RELEASE.2025-04-22T22-12-26Z",
    "minio_source_commit": "0d7408fc9969caf07de6a8c3a84f9fbb10a6739e",
    "python_packages": {d.metadata["Name"]: d.version for d in importlib.metadata.distributions()}}
(results / "versions.json").write_text(json.dumps(versions, indent=2), encoding="utf-8")
print(json.dumps({part: {"jobs": v["number_of_jobs"], "stages": v["number_of_completed_stage_attempts"]} for part, v in summaries.items()}, indent=2))
print("MinIO objects:", len(objects))

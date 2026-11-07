import json
import platform
import time
import urllib.request
from common import session, save


def run():
    spark = session("Jalon1-Partie1-Cluster")
    sc = spark.sparkContext
    try:
        # An action proves execution, beyond merely constructing a session.
        total = sc.parallelize(range(1000), 4).sum()
        with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
            master = json.load(response)
        workers = [w for w in master["workers"] if w["state"] == "ALIVE"]
        assert len(workers) == 2, workers
        assert total == 499500
        result = {"spark_version": spark.version, "master": sc.master, "app_name": sc.appName,
            "application_id": sc.applicationId, "default_parallelism": sc.defaultParallelism,
            "python": platform.python_version(), "alive_workers": len(workers),
            "worker_cores": sum(w["cores"] for w in workers), "test_sum": total,
            "note": "defaultParallelism is a default number of partitions, not a CPU inventory."}
        save("part1_cluster.json", result)
        save("spark_master.json", master)
        print(json.dumps(result, indent=2))
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    run()

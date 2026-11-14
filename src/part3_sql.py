import contextlib
import io
import json
import statistics
from time import perf_counter
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, StructField, StructType
from common import (session, save, parse_event, cents, distance_band, HISTORY,
                    RESTAURANTS, SILVER, ORDER_SCHEMA, RESTAURANT_SCHEMA, RESULTS)


def run():
    spark = session("Jalon1-Partie3-DataFrames-SQL")
    sc = spark.sparkContext
    try:
        sc.setJobGroup("p3-dataframes", "DataFrames et SQL: CA par ville")
        start = perf_counter()
        orders = spark.read.schema(ORDER_SCHEMA).option("mode", "FAILFAST").json(HISTORY)
        restaurants = spark.read.schema(RESTAURANT_SCHEMA).option("mode", "FAILFAST").json(RESTAURANTS)
        delivered = orders.filter(F.col("status") == "DELIVERED")
        revenue = delivered.groupBy("city").agg(F.sum("amount_usd").alias("revenue_usd"))
        df_revenue = {r.city: int(r.revenue_usd * 100) for r in revenue.collect()}
        first_seconds = perf_counter() - start
        orders.createOrReplaceTempView("orders")
        sql_text = "SELECT city, SUM(amount_usd) AS revenue_usd FROM orders WHERE status='DELIVERED' GROUP BY city"
        sql_revenue = {r.city: int(r.revenue_usd * 100) for r in spark.sql(sql_text).collect()}
        part2 = json.loads((RESULTS / "part2_rdd.json").read_text(encoding="utf-8"))
        rdd_revenue = {r["city"]: r["revenue_cents"] for r in part2["cities"]}
        assert df_revenue == sql_revenue == rdd_revenue

        sc.setJobGroup("p3-join", "Jointure avec les 200 restaurants")
        restaurant_rows = restaurants.count()
        assert restaurant_rows == 200
        assert restaurants.select("restaurant_id").distinct().count() == restaurant_rows
        dimension = restaurants.withColumnRenamed("city", "restaurant_city").withColumnRenamed("name", "restaurant_name")
        # Explicit broadcast is justified by this small, measured reference table.
        joined = orders.join(F.broadcast(dimension), "restaurant_id", "inner")
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            joined.explain(True)
        (RESULTS / "part3_explain_join.txt").write_text(stream.getvalue(), encoding="utf-8")
        print(stream.getvalue())
        input_count = orders.count()
        joined_count = joined.count()
        assert joined_count == input_count, "Missing restaurants or row multiplication"
        assert joined.filter(F.col("city") != F.col("restaurant_city")).count() == 0
        category_revenue = (joined.filter(F.col("status") == "DELIVERED")
            .groupBy("city", "category").agg(F.sum("amount_usd").alias("revenue_usd"))
            .orderBy("city", "category").collect())

        udf_band = F.udf(distance_band, StringType())
        def native_band():
            return (F.when(F.col("distance_km").isNull(), F.lit(None).cast("string"))
                .when(F.col("distance_km") < 2, "court")
                .when(F.col("distance_km") <= 5, "moyen").otherwise("long"))
        # Boundary and null checks catch the common 2/5 km off-by-one mistakes.
        boundary = spark.createDataFrame([(None,), (1.99,), (2.0,), (5.0,), (5.01,)],
            StructType([StructField("distance_km", DoubleType())]))
        boundary_rows = boundary.select("distance_km", udf_band("distance_km").alias("udf"), native_band().alias("native")).collect()
        assert all(r.udf == r.native for r in boundary_rows)
        assert {r.distance_km: r.native for r in boundary_rows} == {None: None, 1.99: "court", 2.0: "moyen", 5.0: "moyen", 5.01: "long"}

        def band_action(use_udf):
            expression = udf_band("distance_km") if use_udf else native_band()
            # Grouping consumes the derived column: count() alone could prune it.
            return {r.band: r["count"] for r in orders.select(expression.alias("band")).groupBy("band").count().collect()}
        sc.setJobGroup("p3-benchmark", "Mesures répétées: RDD / DataFrame / SQL et UDF / native")
        def rdd_action():
            return dict(sc.textFile(HISTORY, 4).map(parse_event)
                .filter(lambda r: r["status"] == "DELIVERED")
                .map(lambda r: (r["city"], cents(r))).reduceByKey(lambda a, b: a + b).collect())
        def df_action():
            return {r.city: int(r.revenue_usd * 100) for r in delivered.groupBy("city")
                .agg(F.sum("amount_usd").alias("revenue_usd")).collect()}
        def sql_action():
            return {r.city: int(r.revenue_usd * 100) for r in spark.sql(sql_text).collect()}
        actions = {"RDD": rdd_action, "DataFrame": df_action, "SQL": sql_action,
                   "UDF": lambda: band_action(True), "Native": lambda: band_action(False)}
        expected = {name: action() for name, action in actions.items()}  # warm-up
        assert expected["RDD"] == expected["DataFrame"] == expected["SQL"] == df_revenue
        assert expected["UDF"] == expected["Native"]
        timings = {name: [] for name in actions}
        names = list(actions)
        for iteration in range(3):
            for name in names[iteration:] + names[:iteration]:
                start = perf_counter()
                observed = actions[name]()
                timings[name].append(perf_counter() - start)
                assert observed == expected[name]
        benchmarks = {name: {"seconds": values, "median_seconds": statistics.median(values)}
                      for name, values in timings.items()}

        sc.setJobGroup("p3-parquet", "Écriture Silver Parquet partitionnée par ville et relecture")
        clean = joined.filter(
            F.col("order_id").isNotNull() & F.col("event_time").isNotNull()
            & F.col("status").isin("CREATED", "PICKED_UP", "DELIVERED", "CANCELLED")
            & (F.col("amount_usd") >= 0) & (F.col("items_count") > 0) & (F.col("distance_km").isNull() | (F.col("distance_km") >= 0))
        ).withColumn("distance_band", native_band()).drop("restaurant_city")
        cleaned_count = clean.count()
        # Re-running replaces this project's derived batch output only.
        clean.write.mode("overwrite").partitionBy("city").parquet(SILVER)
        restored = spark.read.parquet(SILVER)
        restored_count = restored.count()
        assert restored_count == cleaned_count == input_count
        assert restored.select("city").distinct().count() == orders.select("city").distinct().count()
        assert {r.city: int(r.revenue_usd * 100) for r in restored.filter(F.col("status") == "DELIVERED")
            .groupBy("city").agg(F.sum("amount_usd").alias("revenue_usd")).collect()} == df_revenue
        result = {"application_id": sc.applicationId, "input_events": input_count,
            "restaurant_rows": restaurant_rows, "joined_rows": joined_count, "silver_rows": restored_count,
            "revenue_cents_dataframe": df_revenue, "revenue_cents_sql": sql_revenue,
            "revenue_by_category": [r.asDict() for r in category_revenue],
            "distance_band_counts": expected["Native"], "boundary_checks_passed": True,
            "benchmarks": benchmarks, "first_dataframe_action_seconds": first_seconds,
            "benchmark_protocol": "Same JSON input, 4 total executor cores, no explicit cache; one warm-up, three timed actions, rotated order. OS/JVM/S3 caches may remain warm. Small local dataset.",
            "silver_path": SILVER, "partition_column": "city",
            "duplicate_policy": "Preserved to keep part 2/3 results comparable. Streaming deduplication belongs to part 5.",
            "join_strategy": "BroadcastHashJoin (explicit broadcast of 200 restaurants)"}
        save("part3_sql.json", result)
        print(json.dumps(result, indent=2, default=str, ensure_ascii=False))
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    run()

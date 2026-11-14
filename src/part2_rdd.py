from time import perf_counter
from common import session, save, parse_event, cents, add_pair, HISTORY, RESULTS


def run():
    spark = session("Jalon1-Partie2-RDD")
    sc = spark.sparkContext
    try:
        sc.setJobGroup("p2-revenue", "RDD: CA et panier moyen par ville")
        start = perf_counter()
        rows = sc.textFile(HISTORY, minPartitions=4).map(parse_event)
        delivered = rows.filter(lambda r: r["status"] == "DELIVERED")
        revenue = delivered.map(lambda r: (r["city"], cents(r))).reduceByKey(lambda a, b: a + b)
        totals = dict(revenue.collect())
        revenue_seconds = perf_counter() - start
        # (sum, count) computes the average with one reduceByKey, no groupByKey.
        stats = delivered.map(lambda r: (r["city"], (cents(r), 1))).reduceByKey(add_pair)
        aggregates = sorted(stats.collect())
        city_rows = [{"city": city, "revenue_cents": amount, "revenue_usd": amount / 100,
                      "delivered_events": count, "average_usd": amount / count / 100}
                     for city, (amount, count) in aggregates]
        assert all(totals[r["city"]] == r["revenue_cents"] for r in city_rows)
        sc.setJobGroup("p2-top5", "RDD: top 5 restaurants")
        top5 = (delivered.map(lambda r: (r["restaurant_id"], 1))
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(5, key=lambda item: (-item[1], item[0])))
        lineage = stats.toDebugString().decode("utf-8")
        (RESULTS / "part2_lineage.txt").write_text(lineage, encoding="utf-8")
        print(lineage)
        result = {"application_id": sc.applicationId, "input_events": rows.count(),
            "delivered_events": delivered.count(), "cities": city_rows,
            "top5_restaurants": [{"restaurant_id": r, "delivered_events": n} for r, n in top5],
            "first_revenue_action_seconds": revenue_seconds,
            "counting_policy": "DELIVERED events, including injected duplicates, as requested in part 2. Not unique-order revenue."}
        save("part2_rdd.json", result)
        for row in city_rows:
            print(row)
        print("TOP 5", top5)
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    run()

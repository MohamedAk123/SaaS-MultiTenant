"""Independent standard-library oracle for the generated batch results."""
import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

root = Path(__file__).resolve().parents[1]
results = root / "results"
rows = [json.loads(line, parse_float=Decimal) for line in (root / "data/orders_history.jsonl").read_text(encoding="utf-8").splitlines()]
restaurants = {r["restaurant_id"]: r for r in map(json.loads, (root / "data/restaurants.json").read_text(encoding="utf-8").splitlines())}
totals, counts, top, categories, bands = defaultdict(int), Counter(), Counter(), defaultdict(int), Counter()
for r in rows:
    assert r["restaurant_id"] in restaurants
    assert r["city"] == restaurants[r["restaurant_id"]]["city"]
    band = "court" if r["distance_km"] < 2 else "moyen" if r["distance_km"] <= 5 else "long"
    bands[band] += 1
    if r["status"] == "DELIVERED":
        amount = int(r["amount_mad"] * 100)
        totals[r["city"]] += amount
        counts[r["city"]] += 1
        top[r["restaurant_id"]] += 1
        categories[(r["city"], restaurants[r["restaurant_id"]]["category"])] += amount
p2 = json.loads((results / "part2_rdd.json").read_text(encoding="utf-8"))
p3 = json.loads((results / "part3_sql.json").read_text(encoding="utf-8"))
assert len(rows) == 50000 and len(restaurants) == 200
assert len(rows) - len({(r["order_id"], r["status"]) for r in rows}) == 1000
assert totals == {r["city"]: r["revenue_cents"] for r in p2["cities"]} == p3["revenue_cents_dataframe"] == p3["revenue_cents_sql"]
assert counts == {r["city"]: r["delivered_events"] for r in p2["cities"]}
assert sum(counts.values()) == p2["delivered_events"]
for r in p2["cities"]:
    assert abs(r["average_mad"] - totals[r["city"]] / counts[r["city"]] / 100) < 1e-9
assert [(r["restaurant_id"], r["delivered_events"]) for r in p2["top5_restaurants"]] == sorted(top.items(), key=lambda p: (-p[1], p[0]))[:5]
assert categories == {(r["city"], r["category"]): int(Decimal(r["revenue_mad"]) * 100) for r in p3["revenue_by_category"]}
assert bands == p3["distance_band_counts"]
assert p3["input_events"] == p3["joined_rows"] == p3["silver_rows"] == 50000
validation = {"status": "PASS", "oracle": "Python standard library, independent of Spark", "checks": [
    "50000 events and 200 restaurants", "1000 exact order/status duplicate rows",
    "Referential integrity and city consistency", "RDD = DataFrame = SQL = independent revenue totals",
    "Counts and average baskets", "Top 5 restaurants with deterministic tie breaking",
    "Category/city revenue", "Distance band counts", "Parquet read-back count and revenue (Spark assertions)"],
    "delivered_events": sum(counts.values()), "revenue_mad": sum(totals.values()) / 100}
(results / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
print(json.dumps(validation, indent=2))

"""Replacement synthetic generator; NOT the instructor's original producer.

Only standard-library dependencies are needed in file mode.
Events have a stable event_time; simulated arrival order introduces late events.
"""
import argparse
import hashlib
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = [
    ("Casablanca", 33.5731, -7.5898, "CAS"),
    ("Rabat", 34.0209, -6.8416, "RAB"),
    ("Marrakech", 31.6295, -7.9811, "MAR"),
    ("Tanger", 35.7595, -5.8340, "TAN"),
    ("Fes", 34.0331, -5.0003, "FES"),
]
CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger"]


def generate(n=50000, seed=2026, output=Path("data")):
    if n < 100:
        raise ValueError("Use at least 100 events")
    rng = random.Random(seed)
    output.mkdir(parents=True, exist_ok=True)
    restaurants = []
    for i in range(200):
        city, lat, lon, _ = CITIES[i % len(CITIES)]
        restaurants.append({
            "restaurant_id": f"R-{i + 1:03d}", "name": f"Restaurant synthétique {i + 1:03d}",
            "city": city, "category": CATEGORIES[(i // 5) % 5],
            "latitude": round(lat + rng.uniform(-.05, .05), 6),
            "longitude": round(lon + rng.uniform(-.05, .05), 6),
        })
    start = datetime(2026, 11, 1, 8, tzinfo=timezone.utc)
    originals = n - round(n * .02)
    events = []
    order_id = 0
    while len(events) < originals:
        order_id += 1
        restaurant = rng.choice(restaurants)
        city_code = next(c[3] for c in CITIES if c[0] == restaurant["city"])
        base = {
            "order_id": f"ORD-{order_id:07d}", "restaurant_id": restaurant["restaurant_id"],
            "customer_id": f"C-{rng.randint(1, 10000):05d}", "city": restaurant["city"],
            "amount_mad": rng.randint(2500, 35000) / 100,
            "items_count": rng.randint(1, 6), "distance_km": round(rng.uniform(.2, 14), 2),
            "courier_id": f"L-{city_code}-{rng.randint(1, 30):02d}",
        }
        states = [("CREATED", 0), ("CANCELLED", 180)] if rng.random() < .12 else [
            ("CREATED", 0), ("PICKED_UP", 300), ("DELIVERED", 900)]
        for status, seconds in states:
            when = start + timedelta(seconds=order_id * 6 + seconds)
            delay = rng.randint(1, 900) if rng.random() < .05 else 0
            row = dict(base, status=status, event_time=when.isoformat().replace("+00:00", "Z"))
            events.append((when + timedelta(seconds=delay), row, delay))
    events.sort(key=lambda e: (e[0], e[1]["order_id"], e[1]["status"]))
    events = events[:originals]
    delayed = sum(delay > 0 for _, _, delay in events)
    duplicated = rng.sample(events, n - originals)
    events += [(arrival + timedelta(seconds=rng.randint(1, 30)), dict(row), delay)
               for arrival, row, delay in duplicated]
    events.sort(key=lambda e: (e[0], e[1]["order_id"], e[1]["status"]))
    for name, rows in [("restaurants.json", restaurants), ("orders_history.jsonl", [e[1] for e in events])]:
        with (output / name).open("w", encoding="utf-8", newline="\n") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    manifest = {
        "source": "synthetic replacement created for this project; not official course data",
        "seed": seed, "events": n, "restaurants": len(restaurants),
        "unique_order_status": originals, "duplicate_rows": n - originals,
        "late_original_events": delayed, "late_original_ratio": delayed / originals,
        "max_simulated_delay_seconds": max(e[2] for e in events),
        "note": "Line order simulates arrival; event_time is business time. Delays are not extra columns.",
        "sha256": {name: hashlib.sha256((output / name).read_bytes()).hexdigest()
                   for name in ["restaurants.json", "orders_history.jsonl"]},
    }
    (output / "generation_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["file"], default="file", help="Jalon 1: file generation only")
    parser.add_argument("--n", type=int, default=50000)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", type=Path, default=Path("data"))
    args = parser.parse_args()
    generate(args.n, args.seed, args.output)

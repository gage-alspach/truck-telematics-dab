import argparse
import json
import os
import random
import time
from datetime import datetime, timezone


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic truck GPS JSON files.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume", default="landing")
    parser.add_argument("--batches", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument(
        "--drift-mode",
        choices=("none", "add_column", "rename_latitude"),
        default="none",
        help="Optional demo mode for schema-drift monitoring.",
    )
    return parser.parse_args()


def make_ping(truck_id: str):
    lat0, lon0 = 41.85, -87.65
    return {
        "truck_id": truck_id,
        "latitude": round(lat0 + random.uniform(-0.2, 0.2), 6),
        "longitude": round(lon0 + random.uniform(-0.2, 0.2), 6),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def apply_drift(row: dict, drift_mode: str) -> dict:
    row = dict(row)
    if drift_mode == "add_column":
        row["fuel_percent"] = random.randint(15, 100)
    elif drift_mode == "rename_latitude":
        row["lat"] = row.pop("latitude")
    return row


def main():
    args = parse_args()
    if args.batches < 1:
        raise ValueError("--batches must be >= 1")
    if args.sleep_seconds < 0:
        raise ValueError("--sleep-seconds must be >= 0")

    landing = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/pings"
    os.makedirs(landing, exist_ok=True)

    trucks = [f"TRK-{i:03d}" for i in range(1, 21)]
    random.seed()

    for batch in range(args.batches):
        rows = [make_ping(random.choice(trucks)) for _ in range(random.randint(5, 15))]
        if rows:
            rows.append(dict(rows[0]))  # intentional duplicate
            bad = make_ping(random.choice(trucks))
            bad["latitude"] = None
            rows.append(bad)  # intentional data-quality violation

        rows = [apply_drift(r, args.drift_mode) for r in rows]

        filename = f"{landing}/pings_{time.time_ns()}_{batch:04d}.json"
        with open(filename, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

        print(f"wrote {filename} ({len(rows)} rows, drift={args.drift_mode})")
        if args.sleep_seconds:
            time.sleep(args.sleep_seconds)

    print("done generating")


if __name__ == "__main__":
    main()

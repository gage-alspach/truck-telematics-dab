import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Print lightweight schema/data-quality health metrics.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--rescued-warn-rate", type=float, default=0.05)
    parser.add_argument("--null-warn-rate", type=float, default=0.20)
    return parser.parse_args()


def safe_rate(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    bronze_name = f"{args.catalog}.{args.schema}.bronze_pings"
    silver_name = f"{args.catalog}.{args.schema}.silver_pings"

    bronze = spark.table(bronze_name)
    cutoff = F.expr(f"current_timestamp() - INTERVAL {args.lookback_minutes} MINUTES")
    recent = bronze.where(F.col("_ingested_at") >= cutoff)
    total = recent.count()

    report = {
        "catalog": args.catalog,
        "schema": args.schema,
        "lookback_minutes": args.lookback_minutes,
        "bronze_rows": total,
        "warnings": [],
    }

    rescued = recent.where(F.col("_rescued_data").isNotNull()).count()
    rescued_rate = safe_rate(rescued, total)
    report["rescued_data_rate"] = rescued_rate
    if rescued_rate >= args.rescued_warn_rate:
        report["warnings"].append(
            f"rescued_data_rate={rescued_rate:.2%} >= {args.rescued_warn_rate:.2%}"
        )

    expected = ["truck_id", "latitude", "longitude", "event_ts"]
    for column in expected:
        if column not in recent.columns:
            null_rate = 1.0
        else:
            nulls = recent.where(F.col(column).isNull()).count()
            null_rate = safe_rate(nulls, total)
        report[f"{column}_null_rate"] = null_rate
        if null_rate >= args.null_warn_rate:
            report["warnings"].append(
                f"{column}_null_rate={null_rate:.2%} >= {args.null_warn_rate:.2%}"
            )

    if spark.catalog.tableExists(silver_name):
        silver = spark.table(silver_name)
        if "_ingested_at" in silver.columns:
            report["silver_rows"] = silver.where(F.col("_ingested_at") >= cutoff).count()
        else:
            report["silver_rows"] = silver.count()

    print(json.dumps(report, indent=2, sort_keys=True))
    if report["warnings"]:
        print("WARNING: review recent Bronze schema/data quality before downstream impact grows.")
    else:
        print("Quality report: no configured warning threshold exceeded.")


if __name__ == "__main__":
    main()

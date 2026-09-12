import argparse
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Idempotently seed truck_details.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    random.seed(42)

    makes = [
        ("Freightliner", "Cascadia"),
        ("Volvo", "VNL"),
        ("Kenworth", "T680"),
        ("Peterbilt", "579"),
    ]
    depots = [
        ("Chicago", "Midwest"),
        ("Dallas", "South"),
        ("Denver", "West"),
        ("Atlanta", "Southeast"),
    ]
    drivers = [
        "A. Rivera",
        "B. Chen",
        "C. Okafor",
        "D. Patel",
        "E. Nguyen",
        "F. Santos",
        "G. Kim",
        "H. Brooks",
        "I. Novak",
        "J. Alvarez",
    ]

    rows = []
    for i in range(1, 21):
        make, model = random.choice(makes)
        depot, region = random.choice(depots)
        rows.append(
            (
                f"TRK-{i:03d}",
                make,
                model,
                random.choice([20000, 26000, 34000, 40000]),
                depot,
                region,
                random.choice(drivers),
                True,
            )
        )

    schema = StructType(
        [
            StructField("truck_id", StringType(), False),
            StructField("make", StringType(), False),
            StructField("model", StringType(), False),
            StructField("capacity_lbs", IntegerType(), False),
            StructField("home_depot", StringType(), False),
            StructField("region", StringType(), False),
            StructField("driver", StringType(), False),
            StructField("active_flag", BooleanType(), False),
        ]
    )

    seed_df = spark.createDataFrame(rows, schema)
    seed_df.createOrReplaceTempView("_truck_details_seed")

    target = f"`{args.catalog}`.`{args.schema}`.`truck_details`"
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING _truck_details_seed AS s
          ON t.truck_id = s.truck_id
        WHEN MATCHED THEN UPDATE SET
          t.make = s.make,
          t.model = s.model,
          t.capacity_lbs = s.capacity_lbs,
          t.home_depot = s.home_depot,
          t.region = s.region,
          t.driver = s.driver,
          t.active_flag = s.active_flag
        WHEN NOT MATCHED THEN INSERT
          (truck_id, make, model, capacity_lbs, home_depot, region, driver, active_flag)
        VALUES
          (s.truck_id, s.make, s.model, s.capacity_lbs, s.home_depot, s.region, s.driver, s.active_flag)
        """
    )

    count = spark.table(f"{args.catalog}.{args.schema}.truck_details").count()
    print(f"Seed complete: {count} truck_details rows in {args.catalog}.{args.schema}")


if __name__ == "__main__":
    main()

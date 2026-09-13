from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("telematics.catalog")
SCHEMA = spark.conf.get("telematics.schema")
LANDING_PATH = spark.conf.get("telematics.landing_path")
SCHEMA_LOCATION = spark.conf.get("telematics.schema_location")
WATERMARK = spark.conf.get("telematics.watermark", "10 minutes")

TRUCK_DETAILS = f"`{CATALOG}`.`{SCHEMA}`.`truck_details`"


@dp.table(
    name="bronze_pings",
    comment="Append-only raw GPS pings ingested from JSON files with Auto Loader.",
)
def bronze_pings():
    """Keep Bronze close to the source and preserve unexpected data."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="silver_pings",
    comment="Typed, valid, deduplicated GPS pings with bounded event-time state.",
)
@dp.expect_or_drop("required_fields", "truck_id IS NOT NULL AND event_ts IS NOT NULL")
@dp.expect_or_drop(
    "valid_coordinates",
    "latitude BETWEEN -90.0 AND 90.0 AND longitude BETWEEN -180.0 AND 180.0",
)
def silver_pings():
    conformed = (
        spark.readStream.table("bronze_pings")
        .select(
            F.col("truck_id").cast("string").alias("truck_id"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.to_timestamp("event_ts").alias("event_ts"),
            F.col("_ingested_at"),
            F.col("_source_file"),
            F.col("_rescued_data"),
        )
    )

    return conformed.withWatermark("event_ts", WATERMARK).dropDuplicates(
        ["truck_id", "event_ts", "latitude", "longitude"]
    )


@dp.table(
    name="gold_pings_enriched",
    comment=(
        "Event-level Gold stream: clean pings enriched with the current static "
        "truck reference snapshot used by this triggered pipeline update."
    ),
)
def gold_pings_enriched():
    pings = spark.readStream.table("silver_pings").alias("p")
    details = F.broadcast(spark.read.table(TRUCK_DETAILS)).alias("d")

    return (
        pings.join(details, F.col("p.truck_id") == F.col("d.truck_id"), "left")
        .select(
            F.col("p.truck_id").alias("truck_id"),
            F.col("p.latitude").alias("latitude"),
            F.col("p.longitude").alias("longitude"),
            F.col("p.event_ts").alias("event_ts"),
            F.col("p._ingested_at").alias("_ingested_at"),
            F.col("p._source_file").alias("_source_file"),
            F.col("d.make").alias("make"),
            F.col("d.model").alias("model"),
            F.col("d.capacity_lbs").alias("capacity_lbs"),
            F.col("d.truck_class").alias("truck_class"),
            F.col("d.home_depot").alias("home_depot"),
            F.col("d.region").alias("region"),
            F.col("d.driver").alias("driver"),
            F.col("d.active_flag").alias("active_flag"),
        )
    )


@dp.materialized_view(
    name="gold_truck_current",
    comment="One current position row per truck, enriched with the latest reference data.",
)
def gold_truck_current():
    silver = spark.read.table("silver_pings")
    details = F.broadcast(spark.read.table(TRUCK_DETAILS)).alias("d")

    latest_window = Window.partitionBy("truck_id").orderBy(
        F.col("event_ts").desc(), F.col("_ingested_at").desc()
    )

    latest = (
        silver.withColumn("_rn", F.row_number().over(latest_window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .alias("p")
    )

    return (
        latest.join(details, F.col("p.truck_id") == F.col("d.truck_id"), "left")
        .select(
            F.col("p.truck_id").alias("truck_id"),
            F.col("p.latitude").alias("latitude"),
            F.col("p.longitude").alias("longitude"),
            F.col("p.event_ts").alias("event_ts"),
            F.col("d.make").alias("make"),
            F.col("d.model").alias("model"),
            F.col("d.capacity_lbs").alias("capacity_lbs"),
            F.col("d.truck_class").alias("truck_class"),
            F.col("d.home_depot").alias("home_depot"),
            F.col("d.region").alias("region"),
            F.col("d.driver").alias("driver"),
            F.col("d.active_flag").alias("active_flag"),
        )
    )

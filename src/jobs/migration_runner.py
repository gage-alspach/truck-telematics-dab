import argparse
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args():
    parser = argparse.ArgumentParser(description="Apply pending SQL migrations.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--phase", choices=("pre", "post"), required=True)
    parser.add_argument("--migration-root", required=True)
    parser.add_argument("--commit-sha", default="local")
    return parser.parse_args()


def validate_identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe {label} identifier: {value!r}")
    return value


def render_sql(raw_sql: str, catalog: str, schema: str) -> str:
    return raw_sql.replace("{{catalog}}", catalog).replace("{{schema}}", schema)


def main():
    args = parse_args()
    catalog = validate_identifier(args.catalog, "catalog")
    schema = validate_identifier(args.schema, "schema")
    phase = args.phase

    spark = SparkSession.builder.getOrCreate()
    migration_table = f"`{catalog}`.`{schema}`.`_schema_migrations`"

    # Bootstrap state is itself version-controlled in this runner. Every target
    # gets its own table because every target uses a different schema.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {migration_table} (
          migration_id STRING,
          checksum STRING,
          phase STRING,
          applied_at TIMESTAMP,
          bundle_target STRING,
          commit_sha STRING
        ) USING DELTA
        COMMENT 'Apply-once migration history for the telematics bundle'
        """
    )

    migration_dir = Path(args.migration_root) / phase
    if not migration_dir.is_dir():
        raise FileNotFoundError(f"Migration directory does not exist: {migration_dir}")
    migration_files = sorted(migration_dir.glob("*.sql"))

    applied_rows = spark.sql(
        f"SELECT migration_id, checksum FROM {migration_table} WHERE phase = '{phase}'"
    ).collect()
    applied = {row["migration_id"]: row["checksum"] for row in applied_rows}

    if not migration_files:
        print(f"No {phase} migrations found. Nothing to do.")
        return

    history_schema = StructType(
        [
            StructField("migration_id", StringType(), False),
            StructField("checksum", StringType(), False),
            StructField("phase", StringType(), False),
            StructField("applied_at", TimestampType(), False),
            StructField("bundle_target", StringType(), False),
            StructField("commit_sha", StringType(), False),
        ]
    )

    for path in migration_files:
        migration_id = path.stem
        raw = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(raw.encode("utf-8")).hexdigest()

        if migration_id in applied:
            if applied[migration_id] != checksum:
                raise RuntimeError(
                    f"Migration {migration_id} was already applied with a different checksum. "
                    "Never edit an applied migration; add a new migration instead."
                )
            print(f"SKIP {migration_id}: already applied")
            continue

        rendered = render_sql(raw, catalog, schema).strip()
        if not rendered:
            raise RuntimeError(f"Migration {migration_id} is empty")

        print(f"APPLY {migration_id} ({phase})")
        # Convention: one top-level SQL statement per migration file. A migration
        # that needs multiple statements can use a Databricks BEGIN ... END block.
        spark.sql(rendered)

        row = [
            (
                migration_id,
                checksum,
                phase,
                datetime.now(timezone.utc),
                args.target,
                args.commit_sha,
            )
        ]
        spark.createDataFrame(row, history_schema).write.mode("append").saveAsTable(
            f"{catalog}.{schema}._schema_migrations"
        )
        print(f"DONE {migration_id}")


if __name__ == "__main__":
    main()

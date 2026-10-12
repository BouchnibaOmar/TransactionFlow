"""Read Silver NYC and publish checked daily Gold Parquet."""

import json

from nyc_io import verify_parquet
from nyc_gold_rules import SOURCE_COLUMNS, reconcile_daily, validate_input_schema
from silver_io import check_output_paths


def check_nyc_gold_paths(silver, output, overwrite=False):
    silver, output = check_output_paths(silver, output, overwrite)
    if not silver.is_dir():
        raise ValueError("--silver must be the trips_clean Parquet directory")
    for name in ["daily_zone_payment", "quality_report.json"]:
        target = output / name
        if target.is_symlink() or (target.exists() and target.resolve().parent != output):
            raise ValueError(f"Output target must not redirect elsewhere: {target}")
    return silver, output


def read_silver(spark, silver):
    trips = spark.read.parquet(str(silver))
    validate_input_schema(trips)
    return trips.select(*SOURCE_COLUMNS)


def write_daily(spark, daily, report, output, overwrite=False):
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "quality_report.json"
    if overwrite and report_path.exists():
        report_path.unlink()
    writer = daily.write.mode("overwrite" if overwrite else "errorifexists")
    if report["group_rows"]:
        writer = writer.partitionBy("year", "month")
    # Empty output retains a readable schema without partition directories.
    writer.parquet(str(output / "daily_zone_payment"))
    # Keep integer partition types even when every date/year/month key is NULL.
    restored = spark.read.schema(daily.schema).parquet(str(output / "daily_zone_payment"))
    report["read_back_groups"] = verify_parquet(restored, daily, report["group_rows"], "daily_zone_payment")
    restored_totals = reconcile_daily(restored, report["source_totals"])
    if restored_totals != report["totals"]:
        raise RuntimeError("Gold NYC totals changed during Parquet write/read")
    report["checks"]["read_back_schema_keys_contents_totals_verified"] = True
    report["output_schema"] = restored.schema.jsonValue()
    with report_path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")

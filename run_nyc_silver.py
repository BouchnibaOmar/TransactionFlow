"""Run Silver NYC while preserving source values and row multiplicity."""

import argparse
import json

from pyspark.sql import SparkSession, functions as F

from nyc_io import check_nyc_paths, read_nyc_inputs, source_fingerprint, write_nyc_outputs
from nyc_rules import classify_trips, enrich_zones, month_bounds, quality_report, validate_zones


def create_spark():
    return (
        SparkSession.builder.master("local[2]").appName("TransactionFlow-NYC-Silver")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true").getOrCreate()
    )


def run_pipeline(spark, input_path, zones_path, month, output, overwrite=False):
    month_bounds(month)
    input_path, zones_path, output = check_nyc_paths(input_path, zones_path, output, overwrite)
    sources = {"trips": source_fingerprint(input_path), "zones": source_fingerprint(zones_path)}
    trips, raw_zones = read_nyc_inputs(spark, input_path, zones_path)
    zones = validate_zones(raw_zones)
    input_rows = trips.count()
    enriched, join_counts = enrich_zones(trips, zones, input_rows)
    classified = classify_trips(enriched, month).cache()
    try:
        report = quality_report(classified, input_rows, join_counts, month)
        report["sources"] = sources
        report["source_schema"] = trips.schema.jsonValue()
        report["zone_source_schema"] = raw_zones.schema.jsonValue()
        report["spark"] = {
            "version": spark.version, "master": spark.sparkContext.master,
            "configuration": {key: spark.conf.get(key) for key in [
                "spark.sql.session.timeZone", "spark.sql.timestampType",
                "spark.sql.shuffle.partitions", "spark.sql.ansi.enabled",
                "spark.sql.adaptive.enabled", "spark.sql.parquet.inferTimestampNTZ.enabled",
            ]},
        }
        clean = classified.filter(F.size("rejection_reasons") == 0)
        quarantined = classified.filter(F.size("rejection_reasons") > 0)
        write_nyc_outputs(spark, clean, quarantined, report, output, overwrite)
        return report
    finally:
        classified.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    month_bounds(args.month)
    check_nyc_paths(args.input, args.zones, args.output, args.overwrite)
    spark = create_spark()
    try:
        report = run_pipeline(spark, args.input, args.zones, args.month, args.output, args.overwrite)
        print(json.dumps({key: report[key] for key in [
            "input_rows", "clean_rows", "quarantined_rows", "rejection_reasons", "quality_flags", "checks",
        ]}, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

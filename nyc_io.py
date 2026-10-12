"""NYC source provenance and checked Parquet writing."""

import hashlib
import json
from pathlib import Path

from nyc_rules import validate_trip_schema
from silver_io import check_output_paths, read_csv

ZONE_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]


def check_nyc_paths(input_path, zones_path, output, overwrite=False):
    input_path, output = check_output_paths(input_path, output, overwrite)
    zones_path, output = check_output_paths(zones_path, output, overwrite)
    for source in [input_path, zones_path]:
        if not source.is_file():
            raise ValueError(f"NYC source must be a file: {source}")
    for name in ["trips_clean", "trips_quarantined", "quality_report.json"]:
        target = output / name
        if target.is_symlink() or (target.exists() and target.resolve().parent != output):
            raise ValueError(f"Output target must not redirect elsewhere: {target}")
    return input_path, zones_path, output


def source_fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return {"path": str(path), "sha256": digest.hexdigest()}


def read_nyc_inputs(spark, input_path, zones_path):
    trips = spark.read.parquet(str(input_path))
    validate_trip_schema(trips)
    zones = read_csv(spark, zones_path, ZONE_COLUMNS)
    return trips, zones


def verify_parquet(frame, expected, expected_rows, name):
    actual_types = {field.name: field.dataType for field in frame.schema.fields}
    expected_types = {field.name: field.dataType for field in expected.schema.fields}
    if actual_types != expected_types:
        raise RuntimeError(f"Parquet schema differs for {name}")
    actual_rows = frame.count()
    if actual_rows != expected_rows:
        raise RuntimeError(f"Parquet row count differs for {name}: {actual_rows} != {expected_rows}")
    # exceptAll checks source values AND multiplicity, without collecting trip rows.
    columns = expected.columns
    if (expected.select(*columns).exceptAll(frame.select(*columns)).limit(1).count()
            or frame.select(*columns).exceptAll(expected.select(*columns)).limit(1).count()):
        raise RuntimeError(f"Parquet values or multiplicities differ for {name}")
    return actual_rows


def write_nyc_outputs(spark, clean, quarantined, report, output, overwrite=False):
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "quality_report.json"
    if overwrite and report_path.exists():
        report_path.unlink()
    mode = "overwrite" if overwrite else "errorifexists"
    writer = clean.write.mode(mode)
    # An empty partitioned write has no data file: write its schema without partitions.
    if report["clean_rows"]:
        writer = writer.partitionBy("year", "month")
    writer.parquet(str(output / "trips_clean"))
    quarantined.write.mode(mode).parquet(str(output / "trips_quarantined"))
    read_clean = spark.read.parquet(str(output / "trips_clean"))
    read_quarantined = spark.read.parquet(str(output / "trips_quarantined"))
    report["read_back_rows"] = {
        "clean": verify_parquet(read_clean, clean, report["clean_rows"], "trips_clean"),
        "quarantined": verify_parquet(read_quarantined, quarantined, report["quarantined_rows"], "trips_quarantined"),
    }
    report["checks"]["parquet_schemas_values_multiplicities_verified"] = True
    report["output_schemas"] = {"clean": read_clean.schema.jsonValue(), "quarantined": read_quarantined.schema.jsonValue()}
    with report_path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")

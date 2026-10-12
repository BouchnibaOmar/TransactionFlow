import csv
import hashlib
import json
from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType,
)

from nyc_io import ZONE_COLUMNS, check_nyc_paths, read_nyc_inputs, verify_parquet
from nyc_rules import (
    classify_trips, enrich_zones, month_bounds, quality_report,
    validate_trip_schema, validate_zones,
)
from run_nyc_silver import create_spark, run_pipeline
from silver_io import read_csv

TRIP_SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("payment_type", LongType()),
    StructField("total_amount", DoubleType()),
    StructField("source_note", StringType()),
])


@pytest.fixture(scope="module")
def spark():
    session = create_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def zones_path(tmp_path):
    path = tmp_path / "zones.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(ZONE_COLUMNS)
        writer.writerows([
            [1, "Manhattan", "Test zone", "Yellow Zone"],
            [2, "Unknown", "N/A", "N/A"],
        ])
    return path


def trip(note="valid", **changes):
    row = {
        "tpep_pickup_datetime": datetime(2025, 1, 1),
        "tpep_dropoff_datetime": datetime(2025, 1, 1, 0, 10),
        "PULocationID": 1, "DOLocationID": 2,
        "trip_distance": 1.5, "payment_type": 1, "total_amount": 10.0,
        "source_note": note,
    }
    row.update(changes)
    return row


def classify(spark, zones_path, records):
    frame = spark.createDataFrame(records, TRIP_SCHEMA)
    zones = validate_zones(read_csv(spark, zones_path, ZONE_COLUMNS))
    enriched, counts = enrich_zones(frame, zones, len(records))
    return classify_trips(enriched, "2025-01"), counts


def test_month_dates_and_multiple_reasons(spark, zones_path):
    records = [
        trip("start_included"),
        trip("last_second", tpep_pickup_datetime=datetime(2025, 1, 31, 23, 59, 59), tpep_dropoff_datetime=datetime(2025, 2, 1)),
        trip("next_month", tpep_pickup_datetime=datetime(2025, 2, 1), tpep_dropoff_datetime=datetime(2025, 2, 1, 0, 10)),
        trip("previous_month", tpep_pickup_datetime=datetime(2024, 12, 31, 23, 59, 59)),
        trip("missing_pickup", tpep_pickup_datetime=None),
        trip("missing_dropoff", tpep_dropoff_datetime=None),
        trip("inverted", tpep_dropoff_datetime=datetime(2024, 12, 31, 23, 59, 59)),
        trip("multiple", tpep_pickup_datetime=datetime(2024, 12, 31), tpep_dropoff_datetime=None, total_amount=float("nan")),
    ]
    classified, counts = classify(spark, zones_path, records)
    reasons = {row.source_note: row.rejection_reasons for row in classified.collect()}
    assert reasons == {
        "start_included": [], "last_second": [],
        "next_month": ["pickup_outside_month"], "previous_month": ["pickup_outside_month"],
        "missing_pickup": ["missing_pickup_datetime"],
        "missing_dropoff": ["missing_dropoff_datetime"], "inverted": ["dropoff_before_pickup"],
        "multiple": ["missing_dropoff_datetime", "pickup_outside_month", "nonfinite_total_amount"],
    }
    report = quality_report(classified, 8, counts, "2025-01")
    assert report["input_rows"] == report["clean_rows"] + report["quarantined_rows"] == 8
    assert (report["clean_rows"], report["quarantined_rows"]) == (2, 6)
    assert sum(report["rejection_reasons"].values()) == 8
    assert classified.schema["tpep_pickup_datetime"].dataType == TimestampNTZType()


def test_retained_values_payment_and_zone_flags(spark, zones_path):
    records = [
        trip("negative", total_amount=-1.005, trip_distance=-2.0, payment_type=0),
        trip("zero", total_amount=0.0, trip_distance=0.0, payment_type=5),
        trip("voided", payment_type=6),
        trip("unmapped", payment_type=99, PULocationID=999, DOLocationID=None),
        trip("missing_payment", payment_type=None),
        trip("positive_tie", total_amount=1.005),
    ]
    classified, _ = classify(spark, zones_path, records)
    rows = {row.source_note: row for row in classified.collect()}
    assert all(not row.rejection_reasons for row in rows.values())
    assert rows["negative"].total_amount == -1.005
    assert str(rows["negative"].total_amount_decimal) == "-1.01"
    assert str(rows["positive_tie"].total_amount_decimal) == "1.01"
    assert rows["negative"].payment_label == "Flex Fare"
    assert rows["negative"].quality_flags == ["negative_total_amount", "negative_trip_distance"]
    assert rows["zero"].payment_label == "Unknown"
    assert rows["zero"].quality_flags == ["zero_total_amount", "zero_trip_distance", "unknown_payment_type"]
    assert rows["voided"].payment_label == "Voided trip"
    assert rows["voided"].quality_flags == []
    assert rows["unmapped"].payment_label == "Unmapped"
    assert rows["unmapped"].quality_flags == ["unknown_payment_type", "missing_pickup_zone", "missing_dropoff_zone"]
    assert rows["unmapped"].pickup_zone is None and rows["unmapped"].dropoff_zone is None
    assert rows["missing_payment"].quality_flags == ["missing_payment_type"]
    assert rows["negative"].pickup_borough == "Manhattan"
    assert rows["negative"].dropoff_borough == "Unknown"
    assert rows["negative"].dropoff_zone == "N/A"


@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("column", ["total_amount", "trip_distance"])
def test_missing_and_nonfinite_numeric(spark, value, column):
    frame = spark.createDataFrame([trip(**{column: value})], TRIP_SCHEMA)
    frame = frame.withColumn("__pickup_zone_present", F.lit(True)).withColumn("__dropoff_zone_present", F.lit(True))
    row = classify_trips(frame, "2025-01").first()
    expected = f"missing_{column}" if value is None else f"nonfinite_{column}"
    assert row.rejection_reasons == [expected]


def test_decimal_overflow_quarantined(spark):
    frame = spark.createDataFrame([trip(total_amount=1e18)], TRIP_SCHEMA)
    frame = frame.withColumn("__pickup_zone_present", F.lit(True)).withColumn("__dropoff_zone_present", F.lit(True))
    row = classify_trips(frame, "2025-01").first()
    assert row.total_amount == 1e18
    assert row.total_amount_decimal is None
    assert row.rejection_reasons == ["amount_decimal_conversion_failed"]


@pytest.mark.parametrize("values,error", [
    (["1", "1"], "Duplicate identifier"), (["1", "01"], "Duplicate identifier"),
    ([None], "Missing identifier"), ([" "], "Missing identifier"),
    (["abc"], "Invalid LocationID"), (["0"], "Invalid LocationID"),
    (["-1"], "Invalid LocationID"), (["1.5"], "Invalid LocationID"),
])
def test_zone_reference_invalid(spark, values, error):
    schema = StructType([StructField(name, StringType()) for name in ZONE_COLUMNS])
    raw = spark.createDataFrame([(value, "B", "Z", "S") for value in values], schema)
    with pytest.raises(ValueError, match=error):
        validate_zones(raw)


def test_schema_checked_before_conversion(spark):
    frame = spark.createDataFrame([trip()], TRIP_SCHEMA)
    validate_trip_schema(frame)
    for name, kind in [("tpep_pickup_datetime", "timestamp_ltz"), ("total_amount", "string"), ("payment_type", "double")]:
        with pytest.raises(ValueError, match="Unexpected NYC type"):
            validate_trip_schema(frame.withColumn(name, F.col(name).cast(kind)))
    with pytest.raises(ValueError, match="Missing NYC source column"):
        validate_trip_schema(frame.drop("DOLocationID"))
    with pytest.raises(ValueError, match="conflict"):
        validate_trip_schema(frame.withColumn("year", F.lit(2025)))


@pytest.mark.parametrize("month", ["2025-1", "2025-13", "wrong"])
def test_month_format_rejected(month):
    with pytest.raises(ValueError, match="YYYY-MM"):
        month_bounds(month)


def test_december_window():
    assert month_bounds("2025-12") == ("2025-12-01 00:00:00", "2026-01-01 00:00:00")


def write_source(spark, tmp_path, records):
    directory = tmp_path / "source_files"
    spark.createDataFrame(records, TRIP_SCHEMA).coalesce(1).write.parquet(str(directory))
    return next(directory.glob("*.parquet"))


@pytest.mark.parametrize("case", ["mixed", "all_clean", "all_quarantined", "empty"])
def test_parquet_round_trip_and_multiplicity(spark, tmp_path, zones_path, case):
    duplicate = trip("duplicate")
    records = {
        "mixed": [duplicate, duplicate, trip("invalid", total_amount=float("nan"))],
        "all_clean": [duplicate, duplicate],
        "all_quarantined": [trip("invalid", total_amount=float("inf"))], "empty": [],
    }[case]
    source = write_source(spark, tmp_path, records)
    original = source.read_bytes()
    output = tmp_path / "output"
    report = run_pipeline(spark, source, zones_path, "2025-01", output)
    clean = spark.read.parquet(str(output / "trips_clean"))
    quarantined = spark.read.parquet(str(output / "trips_quarantined"))
    expected_clean = 2 if case in ["mixed", "all_clean"] else 0
    assert clean.count() == report["clean_rows"] == expected_clean
    assert clean.count() + quarantined.count() == len(records)
    assert clean.schema["total_amount_decimal"].dataType.simpleString() == "decimal(18,2)"
    assert clean.schema["tpep_pickup_datetime"].dataType == TimestampNTZType()
    if expected_clean:
        assert clean.filter(F.col("source_note") == "duplicate").count() == 2
        assert list((output / "trips_clean").glob("year=*/month=*"))
    source_frame, _ = read_nyc_inputs(spark, source, zones_path)
    restored = clean.select(*source_frame.columns).unionByName(quarantined.select(*source_frame.columns))
    assert source_frame.exceptAll(restored).count() == restored.exceptAll(source_frame).count() == 0
    assert json.loads((output / "quality_report.json").read_text()) == report
    assert report["sources"]["trips"]["sha256"] == hashlib.sha256(original).hexdigest()
    assert report["sources"]["zones"]["sha256"] == hashlib.sha256(zones_path.read_bytes()).hexdigest()
    assert report["checks"]["parquet_schemas_values_multiplicities_verified"]
    with pytest.raises(ValueError, match="not empty"):
        run_pipeline(spark, source, zones_path, "2025-01", output)
    assert source.read_bytes() == original


def test_verify_parquet_detects_changed_multiplicity(spark):
    frame = spark.createDataFrame([trip(), trip()], TRIP_SCHEMA)
    changed = spark.createDataFrame([trip(), trip("different")], TRIP_SCHEMA)
    with pytest.raises(RuntimeError, match="multiplicities differ"):
        verify_parquet(changed, frame, 2, "changed")


def test_output_paths_protected(spark, tmp_path, zones_path):
    source = write_source(spark, tmp_path, [trip()])
    for unsafe in [source, source.parent, zones_path, zones_path.parent]:
        with pytest.raises(ValueError, match="separate|directory"):
            check_nyc_paths(source, zones_path, unsafe, overwrite=True)
    output = tmp_path / "output"
    output.mkdir()
    marker = output / "keep.txt"
    marker.write_text("keep")
    with pytest.raises(ValueError, match="not empty"):
        check_nyc_paths(source, zones_path, output)
    (output / "trips_clean").symlink_to(source.parent, target_is_directory=True)
    with pytest.raises(ValueError, match="redirect"):
        check_nyc_paths(source, zones_path, output, overwrite=True)
    assert marker.read_text() == "keep"

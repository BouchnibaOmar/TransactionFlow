import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DecimalType, IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType,
)

from nyc_gold_io import check_nyc_gold_paths
from nyc_gold_rules import LABELS, prepare_daily, reconcile_daily, validate_input_schema
from run_nyc_gold import run_pipeline
from run_nyc_silver import create_spark

SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType()),
    StructField("PULocationID", IntegerType()),
    StructField("payment_type", LongType()),
    StructField("total_amount_decimal", DecimalType(18, 2)),
    StructField("quality_flags", ArrayType(StringType())),
    *[StructField(name, StringType()) for name in LABELS],
])


@pytest.fixture(scope="module")
def spark():
    session = create_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def trip(amount="10.00", **changes):
    row = {
        "tpep_pickup_datetime": datetime(2025, 1, 1, 23, 30),
        "PULocationID": 1, "payment_type": 1, "total_amount_decimal": Decimal(amount),
        "quality_flags": [], "pickup_borough": "Manhattan", "pickup_zone": "Zone A",
        "pickup_service_zone": "Yellow Zone", "payment_label": "Credit card",
    }
    row.update(changes)
    return row


@pytest.fixture
def records():
    duplicate = trip(quality_flags=["one", "two"])
    return [
        duplicate, duplicate,
        trip("-3.25", quality_flags=["negative"]), trip("0.00"),
        trip("5.10", payment_type=2, payment_label="Cash"),
        trip("0.02", tpep_pickup_datetime=datetime(2025, 1, 2), PULocationID=2, pickup_zone="Zone B"),
        trip("0.03", tpep_pickup_datetime=datetime(2025, 1, 2), PULocationID=2, pickup_zone="Zone B"),
        trip("-1.00", PULocationID=None, payment_type=None, pickup_borough=None,
             pickup_zone=None, pickup_service_zone=None, payment_label="Unmapped", quality_flags=["missing"]),
        trip("7.00", tpep_pickup_datetime=datetime(2025, 2, 1), PULocationID=2, pickup_zone="Zone B"),
    ]


def test_hand_calculated_groups_and_null_keys(spark, records):
    frame = spark.createDataFrame(records, SCHEMA)
    daily, report = prepare_daily(frame)
    actual = set(map(tuple, daily.select(
        "pickup_date", "PULocationID", "payment_type", "trip_count",
        "positive_amount_total", "negative_amount_total", "net_amount_total",
        "zero_amount_trip_count", "flagged_trip_count",
    ).collect()))
    assert actual == {
        (date(2025, 1, 1), 1, 1, 4, Decimal("20.00"), Decimal("-3.25"), Decimal("16.75"), 1, 3),
        (date(2025, 1, 1), 1, 2, 1, Decimal("5.10"), Decimal("0.00"), Decimal("5.10"), 0, 0),
        (date(2025, 1, 2), 2, 1, 2, Decimal("0.05"), Decimal("0.00"), Decimal("0.05"), 0, 0),
        (date(2025, 1, 1), None, None, 1, Decimal("0.00"), Decimal("-1.00"), Decimal("-1.00"), 0, 1),
        (date(2025, 2, 1), 2, 1, 1, Decimal("7.00"), Decimal("0.00"), Decimal("7.00"), 0, 0),
    }
    assert report["group_rows"] == 5
    assert report["input_rows"] == 9
    assert report["totals"] == {
        "trip_count": 9, "positive_amount_total": "32.15", "negative_amount_total": "-4.25",
        "net_amount_total": "27.90", "zero_amount_trip_count": 1, "flagged_trip_count": 4,
    }
    assert "currency" not in daily.columns
    assert report["currency_metadata"]["code"] == "USD"
    null_group = daily.filter(F.col("PULocationID").isNull()).first()
    assert null_group.pickup_zone is None and null_group.pickup_borough is None
    assert null_group.payment_label == "Unmapped"
    assert daily.schema["net_amount_total"].dataType == DecimalType(28, 2)


@pytest.mark.parametrize("label", LABELS)
def test_inconsistent_labels_refused(spark, label):
    frame = spark.createDataFrame([trip(), trip(**{label: "Changed"})], SCHEMA)
    with pytest.raises(ValueError, match="Inconsistent"):
        prepare_daily(frame)


def test_null_and_nonnull_label_conflict(spark):
    frame = spark.createDataFrame([trip(pickup_zone=None), trip()], SCHEMA)
    with pytest.raises(ValueError, match="Inconsistent"):
        prepare_daily(frame)


def test_exact_decimal_precision(spark):
    frame = spark.createDataFrame([trip("9007199254740993.01"), trip("0.02"), trip("-0.01")], SCHEMA)
    daily, report = prepare_daily(frame)
    assert daily.first().net_amount_total == Decimal("9007199254740993.02")
    assert report["totals"]["net_amount_total"] == "9007199254740993.02"
    assert report["totals"]["positive_amount_total"] == "9007199254740993.03"


@pytest.mark.parametrize("change", ["identity", "zero_count", "flagged_count", "trip_count", "net_total", "duplicate_key"])
def test_explicit_reconciliation_controls(spark, change):
    daily, report = prepare_daily(spark.createDataFrame([trip()], SCHEMA))
    if change == "identity":
        daily = daily.withColumn("positive_amount_total", F.col("positive_amount_total") + F.lit("1.00").cast("decimal(18,2)"))
    elif change == "zero_count":
        daily = daily.withColumn("zero_amount_trip_count", F.lit(-1).cast("long"))
    elif change == "flagged_count":
        daily = daily.withColumn("flagged_trip_count", F.lit(2).cast("long"))
    elif change == "trip_count":
        daily = daily.withColumn("trip_count", F.lit(2).cast("long"))
    elif change == "net_total":
        one = F.lit("1.00").cast("decimal(18,2)")
        daily = daily.withColumn("net_amount_total", F.col("net_amount_total") + one)
        daily = daily.withColumn("positive_amount_total", F.col("positive_amount_total") + one)
    else:
        daily = daily.unionByName(daily)
    with pytest.raises(RuntimeError):
        reconcile_daily(daily, report["source_totals"])


def test_schema_and_missing_amount_refused(spark):
    frame = spark.createDataFrame([trip()], SCHEMA)
    for name, kind in [("total_amount_decimal", "double"), ("tpep_pickup_datetime", "timestamp_ltz")]:
        with pytest.raises(ValueError):
            validate_input_schema(frame.withColumn(name, F.col(name).cast(kind)))
    with pytest.raises(ValueError, match="NULL"):
        prepare_daily(frame.withColumn("total_amount_decimal", F.lit(None).cast("decimal(18,2)")))


@pytest.mark.parametrize("case", ["nonempty", "empty", "null_keys_only"])
def test_parquet_and_protection(spark, tmp_path, records, case):
    if case == "empty":
        records = []
    elif case == "null_keys_only":
        records = [trip("1.00", tpep_pickup_datetime=None, PULocationID=None, payment_type=None,
                        pickup_borough=None, pickup_zone=None, pickup_service_zone=None, payment_label=None)]
    silver, output = tmp_path / "silver", tmp_path / "gold"
    source = spark.createDataFrame(records, SCHEMA)
    source.write.parquet(str(silver))
    report = run_pipeline(spark, silver, output)
    expected, _ = prepare_daily(source)
    restored = spark.read.schema(expected.schema).parquet(str(output / "daily_zone_payment"))
    assert expected.exceptAll(restored.select(*expected.columns)).count() == 0
    assert restored.select(*expected.columns).exceptAll(expected).count() == 0
    assert json.loads((output / "quality_report.json").read_text()) == report
    assert report["totals"]["trip_count"] == len(records)
    assert report["checks"]["read_back_schema_keys_contents_totals_verified"]
    if case == "nonempty":
        assert list((output / "daily_zone_payment").glob("year=2025/month=*"))
    elif case == "empty":
        assert restored.count() == 0
        assert report["group_rows"] == 0
        assert report["totals"]["net_amount_total"] == "0.00"
    else:
        row = restored.first()
        assert row.pickup_date is None and row.PULocationID is None and row.payment_type is None
    with pytest.raises(ValueError, match="not empty"):
        run_pipeline(spark, silver, output)
    assert run_pipeline(spark, silver, output, overwrite=True)["totals"] == report["totals"]
    assert spark.read.parquet(str(silver)).count() == len(records)


def test_paths_protected(tmp_path):
    silver = tmp_path / "silver"
    silver.mkdir()
    for output in [silver, silver / "nested", tmp_path]:
        with pytest.raises(ValueError, match="separate"):
            check_nyc_gold_paths(silver, output, overwrite=True)
    output = tmp_path / "gold"
    output.mkdir()
    (output / "daily_zone_payment").symlink_to(silver, target_is_directory=True)
    with pytest.raises(ValueError, match="redirect"):
        check_nyc_gold_paths(silver, output, overwrite=True)

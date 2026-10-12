import csv
import json

import pytest
from pyspark.sql import functions as F

from gold_io import check_gold_paths
from gold_rules import join_reference, prepare_gold, reconcile_monthly
from run_gold import run_pipeline
from run_silver import create_spark
from silver_io import CUSTOMER_COLUMNS, MERCHANT_COLUMNS, read_csv


@pytest.fixture(scope="module")
def spark():
    session = create_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def gold_inputs(spark, tmp_path):
    references = tmp_path / "references"
    references.mkdir()
    for name, columns, records in [
        ("customers.csv", CUSTOMER_COLUMNS, [["C1", "FR", "retail"], ["C2", "DE", "premium"]]),
        ("merchants.csv", MERCHANT_COLUMNS, [["M1", "grocery"], ["M2", "travel"]]),
    ]:
        with (references / name).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(columns)
            writer.writerows(records)
    transactions = spark.sql("""
        SELECT * FROM VALUES
        ('T1', 'C1', 'M1', TIMESTAMP '2025-01-01 12:00:00', 100L, 'EUR', 2025, 1),
        ('T2', 'C2', 'M1', TIMESTAMP '2025-01-02 12:00:00', 250L, 'EUR', 2025, 1),
        ('T3', 'C1', 'M2', TIMESTAMP '2025-01-03 12:00:00', 400L, 'EUR', 2025, 1),
        ('T4', 'C1', 'M1', TIMESTAMP '2025-02-01 12:00:00',  50L, 'EUR', 2025, 2),
        ('T5', 'C1', 'M1', TIMESTAMP '2025-01-04 12:00:00', 700L, 'USD', 2025, 1),
        ('T6', 'C2', 'M2', TIMESTAMP '2025-02-02 12:00:00', 900L, 'USD', 2025, 2),
        ('T7', 'C2', 'M2', TIMESTAMP '2025-02-03 12:00:00', 100L, 'USD', 2025, 2)
        AS transactions(transaction_id, customer_id, merchant_id, timestamp,
                        amount_cents, currency, year, month)
    """)
    customers = read_csv(spark, references / "customers.csv", CUSTOMER_COLUMNS)
    merchants = read_csv(spark, references / "merchants.csv", MERCHANT_COLUMNS)
    return transactions, customers, merchants, references


def assert_expected_tables(merchant_monthly, customer_monthly):
    # Hand calculations: EUR = 100 + 250 + 400 + 50; USD = 700 + 900 + 100.
    assert set(map(tuple, merchant_monthly.select(
        "year", "month", "merchant_id", "category", "currency",
        "transaction_count", "total_amount_cents",
    ).collect())) == {
        (2025, 1, "M1", "grocery", "EUR", 2, 350),
        (2025, 1, "M2", "travel", "EUR", 1, 400),
        (2025, 2, "M1", "grocery", "EUR", 1, 50),
        (2025, 1, "M1", "grocery", "USD", 1, 700),
        (2025, 2, "M2", "travel", "USD", 2, 1000),
    }
    assert set(map(tuple, customer_monthly.select(
        "year", "month", "customer_id", "country", "segment", "currency",
        "transaction_count", "total_amount_cents",
    ).collect())) == {
        (2025, 1, "C1", "FR", "retail", "EUR", 2, 500),
        (2025, 1, "C2", "DE", "premium", "EUR", 1, 250),
        (2025, 2, "C1", "FR", "retail", "EUR", 1, 50),
        (2025, 1, "C1", "FR", "retail", "USD", 1, 700),
        (2025, 2, "C2", "DE", "premium", "USD", 2, 1000),
    }


def test_monthly_results_and_currency_controls(gold_inputs):
    transactions, customers, merchants, _ = gold_inputs
    merchant_monthly, customer_monthly, report = prepare_gold(transactions, customers, merchants)
    assert_expected_tables(merchant_monthly, customer_monthly)
    expected_totals = {
        "EUR": {"transaction_count": 4, "total_amount_cents": 800},
        "USD": {"transaction_count": 3, "total_amount_cents": 1700},
    }
    assert report["input_transactions"] == 7
    assert report["join_counts"] == {"customers": 7, "merchants": 7}
    assert report["input_by_currency"] == expected_totals
    for name in ["merchant_monthly", "customer_monthly"]:
        assert report[name] == {"rows": 5, "by_currency": expected_totals}
    assert all(report["checks"].values())


@pytest.mark.parametrize("reference_name", ["customers", "merchants"])
@pytest.mark.parametrize("problem", ["duplicate", "empty"])
def test_reference_identifiers_rejected(gold_inputs, reference_name, problem):
    transactions, customers, merchants, _ = gold_inputs
    reference = customers if reference_name == "customers" else merchants
    identifier = "customer_id" if reference_name == "customers" else "merchant_id"
    if problem == "duplicate":
        reference = reference.unionByName(reference.limit(1))
        expected_error = "Duplicate identifier"
    else:
        reference = reference.withColumn(identifier, F.lit(" "))
        expected_error = "Missing identifier"
    if reference_name == "customers":
        customers = reference
    else:
        merchants = reference
    with pytest.raises(ValueError, match=expected_error):
        prepare_gold(transactions, customers, merchants)


@pytest.mark.parametrize("identifier", ["customer_id", "merchant_id"])
def test_unknown_transaction_reference_rejected(gold_inputs, identifier):
    transactions, customers, merchants, _ = gold_inputs
    transactions = transactions.withColumn(identifier, F.lit("UNKNOWN"))
    with pytest.raises(ValueError, match=f"missing references: {identifier}"):
        prepare_gold(transactions, customers, merchants)


def test_join_count_control_detects_multiplication(gold_inputs):
    transactions, customers, _, _ = gold_inputs
    duplicated = customers.unionByName(customers.limit(1))
    with pytest.raises(RuntimeError, match="Join changed transaction count"):
        join_reference(transactions, duplicated, "customer_id", 7)


@pytest.mark.parametrize("changed_column", ["transaction_count", "total_amount_cents"])
def test_reconciliation_detects_wrong_totals(spark, changed_column):
    monthly = spark.sql("SELECT 'EUR' AS currency, 1L AS transaction_count, 100L AS total_amount_cents")
    changed = monthly.withColumn(changed_column, F.col(changed_column) + 1)
    expected = {"EUR": {"transaction_count": 1, "total_amount_cents": 100}}
    with pytest.raises(RuntimeError, match="do not reconcile"):
        reconcile_monthly(changed, expected, "changed_table")


def test_integer_precision_above_float_range(gold_inputs):
    transactions, customers, merchants, _ = gold_inputs
    transactions = transactions.filter(F.col("transaction_id").isin("T1", "T2"))
    transactions = transactions.withColumn(
        "amount_cents", F.when(F.col("transaction_id") == "T1", F.lit(9007199254740993)).otherwise(F.lit(7))
    )
    merchant_monthly, _, report = prepare_gold(transactions, customers, merchants)
    assert merchant_monthly.first().total_amount_cents == 9007199254741000
    assert report["input_by_currency"]["EUR"]["total_amount_cents"] == 9007199254741000
    assert merchant_monthly.schema["total_amount_cents"].dataType.simpleString() == "bigint"


def test_parquet_round_trip_and_overwrite(spark, tmp_path, gold_inputs):
    transactions, _, _, references = gold_inputs
    silver, output = tmp_path / "silver", tmp_path / "gold"
    transactions.write.partitionBy("year", "month").parquet(str(silver))
    report = run_pipeline(spark, silver, references, output)
    merchant_monthly = spark.read.parquet(str(output / "merchant_monthly"))
    customer_monthly = spark.read.parquet(str(output / "customer_monthly"))
    assert_expected_tables(merchant_monthly, customer_monthly)
    assert json.loads((output / "quality_report.json").read_text()) == report
    for name in ["merchant_monthly", "customer_monthly"]:
        assert list((output / name).glob("year=*/month=*"))
    with pytest.raises(ValueError, match="not empty"):
        run_pipeline(spark, silver, references, output)
    assert run_pipeline(spark, silver, references, output, overwrite=True) == report
    assert spark.read.parquet(str(silver)).count() == 7


def test_empty_input_refused_without_outputs(spark, tmp_path, gold_inputs):
    transactions, _, _, references = gold_inputs
    silver, output = tmp_path / "empty_silver", tmp_path / "gold"
    transactions.limit(0).write.parquet(str(silver))
    with pytest.raises(ValueError, match="Silver input is empty"):
        run_pipeline(spark, silver, references, output)
    assert not output.exists()


def test_output_paths_protected(tmp_path):
    silver, references, output = tmp_path / "silver", tmp_path / "references", tmp_path / "gold"
    silver.mkdir()
    references.mkdir()
    output.mkdir()
    marker = output / "keep.txt"
    marker.write_text("keep")
    with pytest.raises(ValueError, match="not empty"):
        check_gold_paths(silver, references, output)
    for unsafe in [silver, silver / "nested", references, references / "nested", tmp_path]:
        with pytest.raises(ValueError, match="separate"):
            check_gold_paths(silver, references, unsafe, overwrite=True)
    (output / "merchant_monthly").symlink_to(silver, target_is_directory=True)
    with pytest.raises(ValueError, match="redirect"):
        check_gold_paths(silver, references, output, overwrite=True)
    assert marker.read_text() == "keep"

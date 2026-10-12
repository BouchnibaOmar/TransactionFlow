import csv
import json

import pytest
from pyspark.sql import functions as F

from generate_data import generate_data
from run_silver import create_spark, run_pipeline
from silver_io import (CUSTOMER_COLUMNS, MERCHANT_COLUMNS, TRANSACTION_COLUMNS,
                       check_output_paths, read_csv)
from silver_rules import prepare_silver, validate_reference, validate_transactions


@pytest.fixture(scope="module")
def spark():
    session = create_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def generated_data(tmp_path):
    source = tmp_path / "raw"
    generate_data(source, rows=6, seed=42, customers=2, merchants=2)
    return source


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)


def load_fixture(spark, directory, transactions):
    write_csv(directory / "customers.csv", CUSTOMER_COLUMNS, [["C1", "FR", "retail"]])
    write_csv(directory / "merchants.csv", MERCHANT_COLUMNS, [["M1", "grocery"]])
    write_csv(directory / "transactions.csv", TRANSACTION_COLUMNS, transactions)
    return (
        read_csv(spark, directory / "transactions.csv", TRANSACTION_COLUMNS),
        read_csv(spark, directory / "customers.csv", CUSTOMER_COLUMNS),
        read_csv(spark, directory / "merchants.csv", MERCHANT_COLUMNS),
    )


def transaction(identifier="T1", amount="123"):
    return [identifier, "C1", "M1", "2025-01-02T03:04:05Z", amount, "EUR"]


def logical_rows(frame):
    return sorted([tuple(row) for row in frame.collect()], key=repr)


def test_validation_rules_and_raw_values(spark, tmp_path):
    cases = [
        (0, "", "missing_transaction_id"),
        (1, "", "missing_customer_id"),
        (2, " ", "missing_merchant_id"),
        (3, "2025-02-30T12:00:00Z", "invalid_timestamp"),
        (3, "2025-01-02T03:04:05+02:00", "invalid_timestamp"),
        (4, "0", "invalid_amount_cents"),
        (4, "-1", "invalid_amount_cents"),
        (4, "1.5", "invalid_amount_cents"),
        (4, "9223372036854775808", "invalid_amount_cents"),
        (4, "abc", "invalid_amount_cents"),
        (5, "USD", "invalid_currency"),
        (1, "C_UNKNOWN", "unknown_customer_id"),
        (2, "M_UNKNOWN", "unknown_merchant_id"),
    ]
    records = []
    for index, (column, value, reason) in enumerate(cases):
        row = transaction(f"T{index:03d}")
        row[column] = value
        records.append(row)
    records.append(transaction("VALID", "9223372036854775807"))
    multi = transaction("MULTI", "0")
    multi[5] = "USD"
    records.append(multi)
    raw, customers, merchants = load_fixture(spark, tmp_path, records)
    validated = validate_transactions(raw, customers, merchants)
    results = {row["transaction_id"]: row.asDict() for row in validated.collect()}
    for index, (column, value, reason) in enumerate(cases):
        identifier = None if column == 0 else f"T{index:03d}"
        result = results[identifier]
        assert result["rejection_reasons"] == [reason]
        assert result[TRANSACTION_COLUMNS[column]] == (None if value == "" else value)
    assert results["VALID"]["rejection_reasons"] == []
    assert results["VALID"]["parsed_amount_cents"] == 9223372036854775807
    assert results["MULTI"]["rejection_reasons"] == ["invalid_amount_cents", "invalid_currency"]


def test_duplicates_conflicts_and_reconciliation(spark, tmp_path):
    records = [transaction(), transaction(), transaction("T2", "456"),
               transaction("CONFLICT", "10"), transaction("CONFLICT", "10"),
               transaction("CONFLICT", "11"), transaction("CONFLICT", "bad"),
               transaction("SPELLING", "1"), transaction("SPELLING", "01"),
               transaction("INVALID", "0"), transaction("INVALID", "0")]
    raw, customers, merchants = load_fixture(spark, tmp_path, records)
    clean, rejected, report = prepare_silver(raw, customers, merchants)
    assert {row.transaction_id for row in clean.collect()} == {"T1", "T2"}
    assert clean.schema["amount_cents"].dataType.simpleString() == "bigint"
    assert clean.agg(F.sum("amount_cents")).first()[0] == 579
    assert report["input_transactions"] == 11
    assert report["validation_rejected_rows"] == 2
    assert report["conflicting_transaction_ids"] == 2
    assert report["conflict_rejected_rows"] == 6
    assert report["rejected_rows"] == 8
    assert report["exact_duplicates_removed"] == 1
    assert report["final_transactions"] == 2
    assert rejected.count() == 8
    assert {tuple(row.rejection_reasons) for row in rejected.collect()} == {
        ("conflicting_transaction_id",), ("invalid_amount_cents",)}


@pytest.mark.parametrize("identifier,columns", [("customer_id", CUSTOMER_COLUMNS), ("merchant_id", MERCHANT_COLUMNS)])
def test_reference_duplicates_fail_before_join(spark, tmp_path, identifier, columns):
    row = ["ID", *(["FR", "retail"] if identifier == "customer_id" else ["grocery"])]
    path = tmp_path / "reference.csv"
    write_csv(path, columns, [row, row])
    reference = read_csv(spark, path, columns)
    with pytest.raises(ValueError, match="Duplicate identifier"):
        validate_reference(reference, identifier)
    row[0] = ""
    write_csv(path, columns, [row])
    with pytest.raises(ValueError, match="Missing identifier"):
        validate_reference(read_csv(spark, path, columns), identifier)


def test_output_protection(tmp_path):
    source, output = tmp_path / "raw", tmp_path / "silver"
    source.mkdir()
    output.mkdir()
    (output / "marker.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError, match="not empty"):
        check_output_paths(source, output)
    assert check_output_paths(source, output, True) == (source.resolve(), output.resolve())
    for unsafe in [source, source / "nested", tmp_path]:
        with pytest.raises(ValueError, match="separate"):
            check_output_paths(source, unsafe, True)


def test_csv_header_check(spark, tmp_path):
    path = tmp_path / "bad.csv"
    write_csv(path, ["wrong"], [["value"]])
    with pytest.raises(ValueError, match="header"):
        read_csv(spark, path, TRANSACTION_COLUMNS)


def test_generated_data_against_manifest(spark, generated_data):
    source = generated_data
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    raw = read_csv(spark, source / "transactions.csv", TRANSACTION_COLUMNS)
    customers = read_csv(spark, source / "customers.csv", CUSTOMER_COLUMNS)
    merchants = read_csv(spark, source / "merchants.csv", MERCHANT_COLUMNS)
    clean, rejected, report = prepare_silver(raw, customers, merchants)
    expected = manifest["expected_results"]
    assert report["input_transactions"] == manifest["volumes"]["total_transactions"]
    assert report["exact_duplicates_removed"] == expected["exact_duplicate_extra_rows"]
    assert report["validation_rejected_rows"] == sum(expected[key] for key in [
        "invalid_timestamp_rows", "missing_customer_id_rows", "unknown_customer_id_rows"])
    assert report["final_transactions"] == expected["valid_transactions_after_cleaning"]
    assert report["conflicting_transaction_ids"] == report["conflict_rejected_rows"] == 0
    expected_rejected = {(item["transaction_id"], (item["type"],)) for item in manifest["anomalies"] if item["type"] != "exact_duplicate"}
    assert {(row.transaction_id, tuple(row.rejection_reasons)) for row in rejected.collect()} == expected_rejected


@pytest.mark.parametrize("with_anomalies", [False, True])
def test_parquet_round_trip_and_logical_reproducibility(spark, tmp_path, generated_data, with_anomalies):
    source = generated_data
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if not with_anomalies:
        path = source / "transactions.csv"
        with path.open(encoding="utf-8", newline="") as stream:
            records = list(csv.reader(stream))
        write_csv(path, TRANSACTION_COLUMNS, records[1:7])
    source_before = {path.name: path.read_bytes() for path in source.iterdir()}
    reports, cleaned, rejected = [], [], []
    for name in ["first", "second"]:
        output = tmp_path / name
        reports.append(run_pipeline(spark, source, output))
        clean = spark.read.parquet(str(output / "transactions_clean"))
        rejects = spark.read.parquet(str(output / "transactions_rejected"))
        assert clean.count() == manifest["volumes"]["base_valid_transactions"]
        assert rejects.count() == (3 if with_anomalies else 0)
        assert set(rejects.columns) == set(TRANSACTION_COLUMNS + ["rejection_reasons"])
        assert rejects.schema["rejection_reasons"].dataType.simpleString() == "array<string>"
        assert clean.schema["amount_cents"].dataType.simpleString() == "bigint"
        assert {"year", "month"} <= set(clean.columns)
        assert list((output / "transactions_clean").glob("year=*/month=*"))
        assert json.loads((output / "quality_report.json").read_text()) == reports[-1]
        cleaned.append(logical_rows(clean))
        rejected.append(logical_rows(rejects))
        raw = read_csv(spark, source / "transactions.csv", TRANSACTION_COLUMNS)
        valid_raw = raw.filter(~F.col("transaction_id").startswith("A_")).dropDuplicates(TRANSACTION_COLUMNS)
        cleaned_amount = clean.agg(F.sum("amount_cents")).first()[0]
        expected_amount = valid_raw.agg(
            F.sum(F.col("amount_cents").cast("long"))
        ).first()[0]
        assert cleaned_amount == expected_amount
        with pytest.raises(ValueError, match="not empty"):
            run_pipeline(spark, source, output)
    assert reports[0] == reports[1]
    assert cleaned[0] == cleaned[1]
    assert rejected[0] == rejected[1]
    assert source_before == {path.name: path.read_bytes() for path in source.iterdir()}

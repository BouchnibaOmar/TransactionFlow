import csv
import json
from datetime import datetime

import pytest

from generate_data import generate_data


def read_csv(path):
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def test_reproducibility(tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    generate_data(left, rows=24, seed=42, customers=5, merchants=3)
    generate_data(right, rows=24, seed=42, customers=5, merchants=3)
    for name in ["customers.csv", "merchants.csv", "transactions.csv", "manifest.json"]:
        assert (left / name).read_bytes() == (right / name).read_bytes()


def test_data_and_manifest(tmp_path):
    rows = 24
    manifest = generate_data(tmp_path, rows=rows, customers=5, merchants=3)
    assert json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8")) == manifest
    customers = read_csv(tmp_path / "customers.csv")
    merchants = read_csv(tmp_path / "merchants.csv")
    transactions = read_csv(tmp_path / "transactions.csv")
    customer_ids = {row["customer_id"] for row in customers}
    merchant_ids = {row["merchant_id"] for row in merchants}
    assert len(customer_ids) == len(customers) == 5
    assert len(merchant_ids) == len(merchants) == 3
    assert len(transactions) == manifest["volumes"]["total_transactions"] == rows + 4
    assert len({row["transaction_id"] for row in transactions[:rows]}) == rows
    months = set()
    for row in transactions[:rows]:
        assert row["customer_id"] in customer_ids
        assert row["merchant_id"] in merchant_ids
        timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        assert datetime(2025, 1, 1) <= timestamp < datetime(2025, 7, 1)
        months.add(timestamp.month)
    assert months == {1, 2, 3, 4, 5, 6}
    for row in transactions:
        assert row["currency"] == "EUR"
        assert row["amount_cents"].isdigit()
        assert 1 <= int(row["amount_cents"]) <= 100000
        assert row["merchant_id"] in merchant_ids

    duplicate_count = invalid_count = missing_count = unknown_count = 0
    seen = set()
    for row in transactions:
        value = tuple(row.values())
        duplicate = value in seen
        seen.add(value)
        try:
            datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
            invalid = False
        except ValueError:
            invalid = True
        missing = row["customer_id"] == ""
        unknown = not missing and row["customer_id"] not in customer_ids
        assert sum([duplicate, invalid, missing, unknown]) <= 1
        duplicate_count += duplicate
        invalid_count += invalid
        missing_count += missing
        unknown_count += unknown
    assert (duplicate_count, invalid_count, missing_count, unknown_count) == (1, 1, 1, 1)
    for anomaly in manifest["anomalies"]:
        row = transactions[anomaly["line"] - 2]
        assert row["transaction_id"] == anomaly["transaction_id"]
        if anomaly["type"] == "exact_duplicate":
            assert row == transactions[anomaly["source_line"] - 2]
        elif anomaly["type"] == "invalid_timestamp":
            with pytest.raises(ValueError):
                datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        else:
            assert row["customer_id"] == anomaly["value"]
    assert manifest["expected_results"]["valid_transactions_after_cleaning"] == rows


def test_overwrite_protection(tmp_path):
    marker = tmp_path / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError, match="not empty"):
        generate_data(tmp_path, rows=2)
    assert list(tmp_path.iterdir()) == [marker]
    generate_data(tmp_path, rows=2, overwrite=True)
    original = (tmp_path / "transactions.csv").read_bytes()
    generate_data(tmp_path, rows=1, overwrite=True)
    assert len(read_csv(tmp_path / "transactions.csv")) == 5
    assert (tmp_path / "transactions.csv").read_bytes() != original
    assert marker.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("parameter", ["rows", "customers", "merchants"])
@pytest.mark.parametrize("value", [0, -1])
def test_positive_volumes(tmp_path, parameter, value):
    with pytest.raises(ValueError, match="positive"):
        generate_data(tmp_path / "new", **{parameter: value})
    assert not (tmp_path / "new").exists()

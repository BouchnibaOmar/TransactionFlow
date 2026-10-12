"""Generate deterministic CSV fixtures using only the Python standard library."""

import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


TRANSACTION_FIELDS = [
    "transaction_id", "customer_id", "merchant_id",
    "timestamp", "amount_cents", "currency",
]


def write_reference_data(output, random_generator, customers, merchants):
    with (output / "customers.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["customer_id", "country", "segment"])
        for index in range(1, customers + 1):
            writer.writerow([
                f"C{index:06d}",
                random_generator.choice(["FR", "DE", "ES", "IT", "BE"]),
                random_generator.choice(["retail", "premium", "business"]),
            ])
    with (output / "merchants.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["merchant_id", "category"])
        for index in range(1, merchants + 1):
            writer.writerow([
                f"M{index:06d}",
                random_generator.choice([
                    "grocery", "travel", "restaurant", "electronics", "services",
                ]),
            ])


def write_transactions(output, random_generator, rows, customers, merchants):
    start = datetime(2025, 1, 1)
    seconds_in_period = 181 * 24 * 60 * 60  # January through June 2025.

    def make_transaction(index):
        # Stratified dates cover the period even at modest volumes.
        lower = (index - 1) * seconds_in_period // rows
        upper = index * seconds_in_period // rows
        offset = random_generator.randrange(lower, max(lower + 1, upper))
        return [
            f"T{index:09d}",
            f"C{random_generator.randint(1, customers):06d}",
            f"M{random_generator.randint(1, merchants):06d}",
            (start + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            random_generator.randint(1, 100000),
            "EUR",
        ]

    with (output / "transactions.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(TRANSACTION_FIELDS)
        first_transaction = None
        for index in range(1, rows + 1):
            record = make_transaction(index)
            if first_transaction is None:
                first_transaction = record.copy()
            writer.writerow(record)
        return write_anomalies(writer, first_transaction, rows)


def write_anomalies(writer, first_transaction, base_rows):
    writer.writerow(first_transaction)
    invalid_date = first_transaction.copy()
    invalid_date[0], invalid_date[3] = "A_INVALID_DATE", "2025-02-30T12:00:00Z"
    writer.writerow(invalid_date)
    missing_customer = first_transaction.copy()
    missing_customer[0], missing_customer[1] = "A_MISSING_CUSTOMER", ""
    writer.writerow(missing_customer)
    unknown_customer = first_transaction.copy()
    unknown_customer[0], unknown_customer[1] = "A_UNKNOWN_CUSTOMER", "C_UNKNOWN"
    writer.writerow(unknown_customer)
    # Physical line numbers include the CSV header.
    return [
        {
            "type": "exact_duplicate", "line": base_rows + 2, "source_line": 2,
            "transaction_id": first_transaction[0],
            "expected": "Identical to source_line in every column; remove one copy",
        },
        {
            "type": "invalid_timestamp", "line": base_rows + 3,
            "transaction_id": "A_INVALID_DATE", "value": invalid_date[3],
            "expected": "Reject: nonexistent calendar date",
        },
        {
            "type": "missing_customer_id", "line": base_rows + 4,
            "transaction_id": "A_MISSING_CUSTOMER", "value": "",
            "expected": "Reject: empty customer_id",
        },
        {
            "type": "unknown_customer_id", "line": base_rows + 5,
            "transaction_id": "A_UNKNOWN_CUSTOMER", "value": "C_UNKNOWN",
            "expected": "Reject: customer_id absent from customers.csv",
        },
    ]


def write_manifest(output, seed, rows, customers, merchants, anomalies):
    manifest = {
        "schema_version": 1,
        "seed": seed,
        "volumes": {
            "customers": customers, "merchants": merchants,
            "base_valid_transactions": rows, "anomaly_rows": 4,
            "total_transactions": rows + 4,
        },
        "currency": "EUR",
        "timestamp_period": {
            "start_inclusive": "2025-01-01T00:00:00Z",
            "end_exclusive": "2025-07-01T00:00:00Z",
        },
        "line_number_convention": "Physical CSV lines, header is line 1; data row number = line - 1",
        "base_valid_lines": {"first": 2, "last": rows + 1},
        "anomalies": anomalies,
        "expected_results": {
            "exact_duplicate_extra_rows": 1, "invalid_timestamp_rows": 1,
            "missing_customer_id_rows": 1, "unknown_customer_id_rows": 1,
            "rejected_anomaly_rows": 4, "valid_transactions_after_cleaning": rows,
        },
    }
    with (output / "manifest.json").open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
    return manifest


def generate_data(output, rows=10000, seed=42, customers=100, merchants=20, overwrite=False):
    """Write reproducible reference data, base transactions and four anomaly rows."""
    if rows < 1 or customers < 1 or merchants < 1:
        raise ValueError("rows, customers and merchants must be positive")
    output = Path(output)
    if output.exists() and not output.is_dir():
        raise ValueError("output must be a directory")
    if output.exists() and any(output.iterdir()) and not overwrite:
        raise ValueError("output directory is not empty; use --overwrite")
    output.mkdir(parents=True, exist_ok=True)
    random_generator = random.Random(seed)
    # Keep this order: all files share the same seeded random sequence.
    write_reference_data(output, random_generator, customers, merchants)
    anomalies = write_transactions(output, random_generator, rows, customers, merchants)
    return write_manifest(output, seed, rows, customers, merchants, anomalies)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000, help="Number of valid base transactions")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/raw/demo"))
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--merchants", type=int, default=20)
    parser.add_argument("--overwrite", action="store_true", help="Replace the four generated files in a nonempty directory")
    args = parser.parse_args()
    try:
        manifest = generate_data(args.output, args.rows, args.seed, args.customers, args.merchants, args.overwrite)
    except (ValueError, OSError) as error:
        parser.exit(2, f"Error: {error}\n")
    print(f"Generated {manifest['volumes']['total_transactions']} transactions ({args.rows} base + 4 anomalies) in {args.output}")


if __name__ == "__main__":
    main()

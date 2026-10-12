"""Read Gold inputs and write partitioned monthly tables."""

import json
from pathlib import Path

from pyspark.sql.types import LongType

from silver_io import (
    CUSTOMER_COLUMNS, MERCHANT_COLUMNS, TRANSACTION_COLUMNS,
    check_output_paths, read_csv,
)


def check_gold_paths(silver, references, output, overwrite=False):
    silver, output = check_output_paths(silver, output, overwrite)
    references, output = check_output_paths(references, output, overwrite)
    for name in ["merchant_monthly", "customer_monthly", "quality_report.json"]:
        target = output / name
        if target.is_symlink() or (target.exists() and target.resolve().parent != output):
            raise ValueError(f"Output target must not redirect to another directory: {target}")
    return silver, references, output


def read_gold_inputs(spark, silver, references):
    transactions = spark.read.parquet(str(silver))
    required_columns = TRANSACTION_COLUMNS + ["year", "month"]
    missing_columns = sorted(set(required_columns) - set(transactions.columns))
    if missing_columns:
        raise ValueError(f"Missing Silver columns: {missing_columns}")
    if not isinstance(transactions.schema["amount_cents"].dataType, LongType):
        raise ValueError("Silver amount_cents must have type long")
    customers = read_csv(spark, references / "customers.csv", CUSTOMER_COLUMNS)
    merchants = read_csv(spark, references / "merchants.csv", MERCHANT_COLUMNS)
    return transactions.select(*required_columns), customers, merchants


def write_gold_outputs(merchant_monthly, customer_monthly, report, output, overwrite=False):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "quality_report.json"
    if overwrite and report_path.exists():
        # A failed replacement must not retain a previous success report.
        report_path.unlink()
    mode = "overwrite" if overwrite else "errorifexists"
    merchant_monthly.write.mode(mode).partitionBy("year", "month").parquet(
        str(output / "merchant_monthly")
    )
    customer_monthly.write.mode(mode).partitionBy("year", "month").parquet(
        str(output / "customer_monthly")
    )
    with report_path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")

"""Run the local TransactionFlow Silver pipeline."""

import argparse
import json

from pyspark.sql import SparkSession

from silver_io import (
    CUSTOMER_COLUMNS, MERCHANT_COLUMNS, TRANSACTION_COLUMNS,
    check_output_paths, read_csv, write_outputs,
)
from silver_rules import prepare_silver


def create_spark():
    return (
        SparkSession.builder.master("local[2]").appName("TransactionFlow-Silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true").getOrCreate()
    )


def run_pipeline(spark, source, output, overwrite=False):
    source, output = check_output_paths(source, output, overwrite)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    customers = read_csv(spark, source / "customers.csv", CUSTOMER_COLUMNS)
    merchants = read_csv(spark, source / "merchants.csv", MERCHANT_COLUMNS)
    transactions = read_csv(spark, source / "transactions.csv", TRANSACTION_COLUMNS)
    clean, rejected, report = prepare_silver(transactions, customers, merchants)
    write_outputs(clean, rejected, report, output, overwrite)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/raw/demo")
    parser.add_argument("--output", default="data/silver/demo")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    # Check paths before starting Java or writing any output.
    check_output_paths(args.input, args.output, args.overwrite)
    spark = create_spark()
    try:
        report = run_pipeline(spark, args.input, args.output, args.overwrite)
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

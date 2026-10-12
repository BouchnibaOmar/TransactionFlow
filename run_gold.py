"""Run the local TransactionFlow Gold pipeline."""

import argparse
import json

from gold_io import check_gold_paths, read_gold_inputs, write_gold_outputs
from gold_rules import prepare_gold
from run_silver import create_spark


def run_pipeline(spark, silver, references, output, overwrite=False):
    silver, references, output = check_gold_paths(silver, references, output, overwrite)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Native long sums fail on overflow instead of silently wrapping.
    spark.conf.set("spark.sql.ansi.enabled", "true")
    transactions, customers, merchants = read_gold_inputs(spark, silver, references)
    merchant_monthly, customer_monthly, report = prepare_gold(transactions, customers, merchants)
    write_gold_outputs(merchant_monthly, customer_monthly, report, output, overwrite)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", default="data/silver/demo_wsl/transactions_clean")
    parser.add_argument("--references", default="data/raw/demo")
    parser.add_argument("--output", default="data/gold/demo_wsl")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    check_gold_paths(args.silver, args.references, args.output, args.overwrite)
    spark = create_spark()
    try:
        report = run_pipeline(spark, args.silver, args.references, args.output, args.overwrite)
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

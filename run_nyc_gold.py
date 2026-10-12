"""Run daily NYC recorded-amount aggregations from trips_clean."""

import argparse
import json

from nyc_gold_io import check_nyc_gold_paths, read_silver, write_daily
from nyc_gold_rules import prepare_daily
from run_nyc_silver import create_spark


def run_pipeline(spark, silver, output, overwrite=False):
    silver, output = check_nyc_gold_paths(silver, output, overwrite)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    trips = read_silver(spark, silver)
    daily, report = prepare_daily(trips)
    report["silver_path"] = str(silver)
    report["spark"] = {
        "version": spark.version, "master": spark.sparkContext.master,
        "configuration": {key: spark.conf.get(key) for key in [
            "spark.sql.session.timeZone", "spark.sql.timestampType", "spark.sql.ansi.enabled",
            "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
        ]},
    }
    write_daily(spark, daily, report, output, overwrite)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    check_nyc_gold_paths(args.silver, args.output, args.overwrite)
    spark = create_spark()
    try:
        report = run_pipeline(spark, args.silver, args.output, args.overwrite)
        print(json.dumps({key: report[key] for key in ["input_rows", "group_rows", "totals", "checks"]}, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

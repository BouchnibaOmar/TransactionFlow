"""CSV reading and Silver output writing."""

import csv
import json
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

TRANSACTION_COLUMNS = [
    "transaction_id", "customer_id", "merchant_id",
    "timestamp", "amount_cents", "currency",
]
CUSTOMER_COLUMNS = ["customer_id", "country", "segment"]
MERCHANT_COLUMNS = ["merchant_id", "category"]


def read_csv(spark, path, columns):
    path = Path(path).resolve()
    with path.open(encoding="utf-8", newline="") as stream:
        if next(csv.reader(stream), None) != columns:
            raise ValueError(f"Unexpected CSV header: {path.name}; expected {columns}")
    # Keep strings until validation so failed conversions do not erase evidence.
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    return (
        spark.read.schema(schema).option("header", True)
        .option("mode", "FAILFAST").option("enforceSchema", False)
        .option("ignoreLeadingWhiteSpace", False)
        .option("ignoreTrailingWhiteSpace", False)
        .csv(str(path))
    )


def check_output_paths(source, output, overwrite=False):
    source, output = Path(source).resolve(), Path(output).resolve()
    # Refuse either nesting direction, including paths resolved through symlinks.
    if source == output or source in output.parents or output in source.parents:
        raise ValueError("Source and output directories must be separate and not nested")
    if output.exists() and not output.is_dir():
        raise ValueError("Output must be a directory")
    if output.exists() and any(output.iterdir()) and not overwrite:
        raise ValueError("Output directory is not empty; use --overwrite")
    for name in ["transactions_clean", "transactions_rejected", "quality_report.json"]:
        target = output / name
        if target.is_symlink() or (target.exists() and target.resolve().parent != output):
            raise ValueError(f"Output target must not redirect to another directory: {target}")
    return source, output


def write_outputs(clean, rejected, report, output, overwrite=False):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "quality_report.json"
    if overwrite and report_path.exists():
        # A failed rerun must not leave a previous report claiming success.
        report_path.unlink()
    mode = "overwrite" if overwrite else "errorifexists"
    clean.write.mode(mode).partitionBy("year", "month").parquet(str(output / "transactions_clean"))
    # The round-trip test also reads this output when there are no rejected rows.
    rejected.write.mode(mode).parquet(str(output / "transactions_rejected"))
    with report_path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")

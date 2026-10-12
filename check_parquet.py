"""Check local Spark Parquet writes and reads before running Silver."""

from pathlib import Path
from pyspark.sql import SparkSession


def main():
    output = Path("data/parquet_check").resolve()
    spark = SparkSession.builder.master("local[2]").appName("TransactionFlow-Parquet-check").getOrCreate()
    try:
        spark.range(10).write.mode("overwrite").parquet(str(output))
        count = spark.read.parquet(str(output)).count()
        if count != 10:
            raise RuntimeError(f"Parquet row count mismatch: expected 10, got {count}")
        print(f"Parquet write/read verified: {count} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

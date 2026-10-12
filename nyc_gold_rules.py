"""Daily NYC recorded amounts, preserving every Silver trip in the totals."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, ByteType, DecimalType, IntegerType, LongType, ShortType, StringType, TimestampNTZType,
)

KEYS = ["pickup_date", "PULocationID", "payment_type"]
LABELS = ["pickup_borough", "pickup_zone", "pickup_service_zone", "payment_label"]
AMOUNTS = ["positive_amount_total", "negative_amount_total", "net_amount_total"]
SOURCE_COLUMNS = ["tpep_pickup_datetime", "PULocationID", "payment_type", "total_amount_decimal", "quality_flags"] + LABELS


def validate_input_schema(trips):
    integral = (ByteType, ShortType, IntegerType, LongType)
    expected_types = {
        "tpep_pickup_datetime": (TimestampNTZType,),
        "PULocationID": integral, "payment_type": integral,
        "total_amount_decimal": (DecimalType,), "quality_flags": (ArrayType,),
        **{name: (StringType,) for name in LABELS},
    }
    if len(set(name.casefold() for name in trips.columns)) != len(trips.columns):
        raise ValueError("Duplicate Silver NYC column names")
    for name, types in expected_types.items():
        if name not in trips.columns or not isinstance(trips.schema[name].dataType, types):
            raise ValueError(f"Missing or unexpected Silver NYC type: {name}")
    if trips.schema["total_amount_decimal"].dataType != DecimalType(18, 2):
        raise ValueError("Silver NYC total_amount_decimal must be decimal(18,2)")
    if trips.schema["quality_flags"].dataType.elementType != StringType():
        raise ValueError("Silver NYC quality_flags must be an array of strings")


def consistent_labels(trips):
    labels = trips.select(*KEYS, *LABELS).distinct()
    if labels.groupBy(*KEYS).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Inconsistent zone or payment labels for a daily zone/payment key")
    return labels


def aggregate_daily(trips):
    amount = F.col("total_amount_decimal")
    zero = F.lit("0.00").cast("decimal(18,2)")
    return trips.groupBy(*KEYS).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum(F.when(amount > zero, amount).otherwise(zero)).alias("positive_amount_total"),
        F.sum(F.when(amount < zero, amount).otherwise(zero)).alias("negative_amount_total"),
        F.sum(amount).alias("net_amount_total"),
        F.sum(F.when(amount == zero, 1).otherwise(0)).alias("zero_amount_trip_count"),
        F.sum(F.when(F.coalesce(F.size("quality_flags"), F.lit(0)) > 0, 1).otherwise(0)).alias("flagged_trip_count"),
    )


def attach_labels(daily, labels):
    left, right = daily.alias("daily"), labels.alias("labels")
    # Ordinary equality would lose keys containing NULL.
    condition = F.lit(True)
    for name in KEYS:
        condition = condition & F.col(f"daily.{name}").eqNullSafe(F.col(f"labels.{name}"))
    return left.join(right, condition, "inner").select(
        "daily.*", *[F.col(f"labels.{name}") for name in LABELS]
    )


def validate_daily_groups(daily):
    if daily.groupBy(*KEYS).count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError("Daily zone/payment keys are not unique")
    identity = (F.col("positive_amount_total") + F.col("negative_amount_total")).eqNullSafe(F.col("net_amount_total"))
    invalid = ~identity | (F.col("positive_amount_total") < 0) | (F.col("negative_amount_total") > 0)
    for name in ["zero_amount_trip_count", "flagged_trip_count"]:
        invalid = invalid | F.col(name).isNull() | (F.col(name) < 0) | (F.col(name) > F.col("trip_count"))
    for name in AMOUNTS + ["trip_count"]:
        invalid = invalid | F.col(name).isNull()
    invalid = invalid | (F.col("trip_count") < 1)
    if daily.filter(invalid).limit(1).count():
        raise RuntimeError("Daily group amount identity or count bounds failed")


def source_totals(trips):
    zero = F.lit("0.00").cast("decimal(38,2)")
    row = trips.agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.coalesce(F.sum("total_amount_decimal"), zero).alias("net_amount_total"),
    ).first()
    return {"trip_count": int(row.trip_count), "net_amount_total": format(row.net_amount_total, ".2f")}


def reconcile_daily(daily, expected_totals):
    validate_daily_groups(daily)
    zero = F.lit("0.00").cast("decimal(38,2)")
    row = daily.agg(
        F.coalesce(F.sum("trip_count"), F.lit(0).cast("long")).alias("trip_count"),
        *[F.coalesce(F.sum(name), zero).alias(name) for name in AMOUNTS],
        F.coalesce(F.sum("zero_amount_trip_count"), F.lit(0).cast("long")).alias("zero_amount_trip_count"),
        F.coalesce(F.sum("flagged_trip_count"), F.lit(0).cast("long")).alias("flagged_trip_count"),
    ).first()
    totals = {name: format(row[name], ".2f") for name in AMOUNTS}
    totals.update({name: int(row[name]) for name in ["trip_count", "zero_amount_trip_count", "flagged_trip_count"]})
    if totals["trip_count"] != expected_totals["trip_count"] or totals["net_amount_total"] != expected_totals["net_amount_total"]:
        raise RuntimeError(f"Gold NYC totals differ from Silver: {totals}, expected {expected_totals}")
    return totals


def prepare_daily(trips):
    validate_input_schema(trips)
    if trips.filter(F.col("total_amount_decimal").isNull()).limit(1).count():
        raise ValueError("Silver NYC normalized amounts must not be NULL")
    trips = trips.select(*SOURCE_COLUMNS).withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    labels = consistent_labels(trips)
    aggregates = aggregate_daily(trips)
    group_count = aggregates.count()
    daily = attach_labels(aggregates, labels).withColumn("year", F.year("pickup_date")).withColumn("month", F.month("pickup_date"))
    if daily.count() != group_count:
        raise RuntimeError("Attaching labels changed the number of daily groups")
    expected_totals = source_totals(trips)
    totals = reconcile_daily(daily, expected_totals)
    report = {
        "input_rows": expected_totals["trip_count"], "group_rows": group_count,
        "source_totals": expected_totals, "totals": totals,
        "amount_meaning": "recorded_amounts_not_certified_revenue_refunds_or_fraud",
        "currency_metadata": {"code": "USD", "origin": "TLC dataset monetary units; not a source column"},
        "checks": {"labels_consistent_per_key": True, "label_join_preserves_groups": True,
                   "keys_unique": True, "trip_counts_reconciled": True, "net_amount_reconciled": True,
                   "group_amount_identity": True, "group_counter_bounds": True},
    }
    return daily, report

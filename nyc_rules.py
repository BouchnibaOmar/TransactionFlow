"""NYC trip schema checks, zone enrichment and row classification."""

import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, DoubleType, IntegerType, LongType, ShortType, TimestampNTZType

from silver_rules import validate_reference

REJECTION_REASONS = [
    "missing_pickup_datetime", "missing_dropoff_datetime", "dropoff_before_pickup",
    "pickup_outside_month", "missing_total_amount", "nonfinite_total_amount",
    "missing_trip_distance", "nonfinite_trip_distance", "amount_decimal_conversion_failed",
]
QUALITY_FLAGS = [
    "negative_total_amount", "zero_total_amount", "negative_trip_distance",
    "zero_trip_distance", "missing_payment_type", "unknown_payment_type",
    "missing_pickup_zone", "missing_dropoff_zone",
]
PAYMENT_LABELS = {
    0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute", 5: "Unknown", 6: "Voided trip",
}
ZONE_ATTRIBUTES = ["borough", "zone", "service_zone"]
DERIVED_COLUMNS = [
    "year", "month", "total_amount_decimal", "payment_label", "quality_flags",
    "rejection_reasons",
] + [f"{side}_{name}" for side in ["pickup", "dropoff"] for name in ZONE_ATTRIBUTES]
ZONE_TEMP_COLUMNS = [f"__{side}_zone_{name}" for side in ["pickup", "dropoff"] for name in ["id", "present"]]


def month_bounds(month):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("month must use YYYY-MM")
    start = datetime.strptime(month, "%Y-%m")
    next_month = datetime(start.year + 1, 1, 1) if start.month == 12 else datetime(start.year, start.month + 1, 1)
    return start.isoformat(sep=" "), next_month.isoformat(sep=" ")


def validate_trip_schema(trips):
    integral_types = (ByteType, ShortType, IntegerType, LongType)
    expected_types = {
        "tpep_pickup_datetime": (TimestampNTZType,),
        "tpep_dropoff_datetime": (TimestampNTZType,),
        "PULocationID": integral_types, "DOLocationID": integral_types,
        "payment_type": integral_types,
        "trip_distance": (DoubleType,), "total_amount": (DoubleType,),
    }
    if len(set(trips.columns)) != len(trips.columns):
        raise ValueError("Duplicate source column names")
    for name, types in expected_types.items():
        if name not in trips.columns:
            raise ValueError(f"Missing NYC source column: {name}")
        if not isinstance(trips.schema[name].dataType, types):
            raise ValueError(f"Unexpected NYC type for {name}: {trips.schema[name].dataType.simpleString()}")
    collisions = sorted(set(trips.columns) & set(DERIVED_COLUMNS + ZONE_TEMP_COLUMNS))
    if collisions:
        raise ValueError(f"Source columns conflict with derived columns: {collisions}")


def validate_zones(raw_zones):
    validate_reference(raw_zones, "LocationID")
    zones = raw_zones.withColumn("zone_id", F.col("LocationID").try_cast("long"))
    invalid_id = (
        ~F.col("LocationID").rlike(r"^[0-9]+$")
        | F.col("zone_id").isNull() | (F.col("zone_id") <= 0)
    )
    if zones.filter(invalid_id).limit(1).count():
        raise ValueError("Invalid LocationID in zone reference; expected a positive integer")
    # Different spellings such as 1 and 01 must not multiply a join.
    validate_reference(zones, "zone_id")
    return zones.select("zone_id", "Borough", "Zone", "service_zone")


def enrich_zones(trips, zones, input_rows):
    join_counts = {}
    for side, source_id in [("pickup", "PULocationID"), ("dropoff", "DOLocationID")]:
        zone_id = f"__{side}_zone_id"
        lookup = zones.select(
            F.col("zone_id").alias(zone_id),
            F.col("Borough").alias(f"{side}_borough"),
            F.col("Zone").alias(f"{side}_zone"),
            F.col("service_zone").alias(f"{side}_service_zone"),
            F.lit(True).alias(f"__{side}_zone_present"),
        )
        trips = trips.join(F.broadcast(lookup), F.col(source_id) == F.col(zone_id), "left").drop(zone_id)
        join_counts[side] = trips.count()
        if join_counts[side] != input_rows:
            raise RuntimeError(f"{side} zone join changed row count: {input_rows} -> {join_counts[side]}")
    return trips, join_counts


def condition_labels(conditions):
    candidates = F.array(*[F.when(condition, F.lit(label)) for condition, label in conditions])
    return F.filter(candidates, lambda value: value.isNotNull())


def nonfinite(column):
    return F.isnan(column) | (F.abs(column) == F.lit(float("inf")))


def classify_trips(trips, month):
    start, end = month_bounds(month)
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    amount, distance, payment = F.col("total_amount"), F.col("trip_distance"), F.col("payment_type")
    amount_finite = amount.isNotNull() & ~nonfinite(amount)
    # Spark round uses HALF_UP; try_cast quarantines decimal overflow without aborting.
    trips = trips.withColumn(
        "total_amount_decimal", F.when(amount_finite, F.round(amount, 2).try_cast("decimal(18,2)"))
    )
    invalid_conditions = [
        (pickup.isNull(), "missing_pickup_datetime"),
        (dropoff.isNull(), "missing_dropoff_datetime"),
        (dropoff < pickup, "dropoff_before_pickup"),
        ((pickup < F.lit(start).cast("timestamp_ntz")) | (pickup >= F.lit(end).cast("timestamp_ntz")), "pickup_outside_month"),
        (amount.isNull(), "missing_total_amount"),
        (nonfinite(amount), "nonfinite_total_amount"),
        (distance.isNull(), "missing_trip_distance"),
        (nonfinite(distance), "nonfinite_trip_distance"),
        (amount_finite & F.col("total_amount_decimal").isNull(), "amount_decimal_conversion_failed"),
    ]
    flag_conditions = [
        (amount < 0, "negative_total_amount"), (amount == 0, "zero_total_amount"),
        (distance < 0, "negative_trip_distance"), (distance == 0, "zero_trip_distance"),
        (payment.isNull(), "missing_payment_type"),
        ((payment == 5) | ~payment.isin(*PAYMENT_LABELS), "unknown_payment_type"),
        (F.col("__pickup_zone_present").isNull(), "missing_pickup_zone"),
        (F.col("__dropoff_zone_present").isNull(), "missing_dropoff_zone"),
    ]
    payment_mapping = F.create_map(*[
        value for code, label in PAYMENT_LABELS.items()
        for value in [F.lit(code).cast("long"), F.lit(label)]
    ])
    return (
        trips.withColumn("rejection_reasons", condition_labels(invalid_conditions))
        .withColumn("quality_flags", condition_labels(flag_conditions))
        .withColumn("payment_label", F.coalesce(F.element_at(payment_mapping, payment.cast("long")), F.lit("Unmapped")))
        .withColumn("year", F.year(pickup)).withColumn("month", F.month(pickup))
        .drop("__pickup_zone_present", "__dropoff_zone_present")
    )


def label_counts(frame, column, labels):
    counts = frame.agg(*[
        F.coalesce(F.sum(F.when(F.array_contains(F.col(column), label), 1).otherwise(0)), F.lit(0)).alias(label)
        for label in labels
    ]).first()
    return {label: int(counts[label]) for label in labels}


def quality_report(classified, input_rows, join_counts, month):
    counts = classified.agg(
        F.sum(F.when(F.size("rejection_reasons") == 0, 1).otherwise(0)).alias("clean"),
        F.sum(F.when(F.size("rejection_reasons") > 0, 1).otherwise(0)).alias("quarantined"),
    ).first()
    clean_rows, quarantined_rows = int(counts.clean or 0), int(counts.quarantined or 0)
    if input_rows != clean_rows + quarantined_rows:
        raise RuntimeError(f"NYC row counts do not reconcile: {input_rows} != {clean_rows} + {quarantined_rows}")
    return {
        "input_rows": input_rows, "clean_rows": clean_rows, "quarantined_rows": quarantined_rows,
        "requested_month": month, "join_counts": join_counts,
        "rejection_reasons": label_counts(classified, "rejection_reasons", REJECTION_REASONS),
        "quality_flags": label_counts(classified.filter(F.size("rejection_reasons") == 0), "quality_flags", QUALITY_FLAGS),
        "quality_flags_scope": "clean_rows_only", "label_counts_can_overlap": True,
        "amount_rounding": "Spark round(total_amount, 2): HALF_UP, then try_cast decimal(18,2)",
        "pickup_outside_month_meaning": "analytic_scope_exclusion_not_proof_of_source_error",
        "checks": {"zone_identifiers_valid_unique": True, "zone_join_counts_preserved": True, "rows_reconciled": True},
    }

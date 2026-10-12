"""Native Spark validation and exact transaction deduplication."""

from pyspark.sql import functions as F

from silver_io import TRANSACTION_COLUMNS


def is_missing_identifier(column):
    return column.isNull() | (F.length(F.trim(column)) == 0)


def validate_reference(reference, identifier):
    if reference.filter(is_missing_identifier(F.col(identifier))).limit(1).count():
        raise ValueError(f"Missing identifier in reference: {identifier}")
    if reference.groupBy(identifier).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Duplicate identifier in reference: {identifier}")


def find_conflicting_ids(transactions):
    # Compare raw contents before validation: an invalid version is also a conflict.
    return (
        transactions.filter(~is_missing_identifier(F.col("transaction_id")))
        .select(*TRANSACTION_COLUMNS).distinct()
        .groupBy("transaction_id").count()
        .filter(F.col("count") > 1).select("transaction_id")
    )


def validate_transactions(transactions, customers, merchants):
    parsed = transactions.withColumn(
        "parsed_timestamp",
        F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")),
    ).withColumn("parsed_amount_cents", F.col("amount_cents").try_cast("long"))
    customer_keys = customers.select("customer_id").withColumn("customer_exists", F.lit(True))
    merchant_keys = merchants.select("merchant_id").withColumn("merchant_exists", F.lit(True))
    parsed = parsed.join(customer_keys, "customer_id", "left").join(merchant_keys, "merchant_id", "left")

    customer_id_missing = is_missing_identifier(F.col("customer_id"))
    merchant_id_missing = is_missing_identifier(F.col("merchant_id"))
    timestamp_format_valid = F.coalesce(
        F.col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"), F.lit(False)
    )
    timestamp_invalid = F.col("parsed_timestamp").isNull() | ~timestamp_format_valid
    amount_has_only_digits = F.coalesce(
        F.col("amount_cents").rlike(r"^[0-9]+$"), F.lit(False)
    )
    amount_invalid = (
        F.col("parsed_amount_cents").isNull()
        | ~amount_has_only_digits
        | (F.col("parsed_amount_cents") <= 0)
    )
    currency_is_eur = F.coalesce(F.col("currency") == "EUR", F.lit(False))
    customer_unknown = ~customer_id_missing & F.col("customer_exists").isNull()
    merchant_unknown = ~merchant_id_missing & F.col("merchant_exists").isNull()

    rules = [
        (is_missing_identifier(F.col("transaction_id")), "missing_transaction_id"),
        (customer_id_missing, "missing_customer_id"),
        (merchant_id_missing, "missing_merchant_id"),
        (timestamp_invalid, "invalid_timestamp"),
        (amount_invalid, "invalid_amount_cents"),
        (~currency_is_eur, "invalid_currency"),
        (customer_unknown, "unknown_customer_id"),
        (merchant_unknown, "unknown_merchant_id"),
    ]
    reason_candidates = F.array(*[
        F.when(condition, F.lit(reason)) for condition, reason in rules
    ])
    reasons = F.filter(reason_candidates, lambda reason: reason.isNotNull())
    return parsed.withColumn("rejection_reasons", reasons).drop("customer_exists", "merchant_exists")


def deduplicate_transactions(validated):
    # Raw equality avoids treating different spellings of an amount as exact copies.
    unique = validated.dropDuplicates(TRANSACTION_COLUMNS)
    return unique.select(
        "transaction_id", "customer_id", "merchant_id",
        F.col("parsed_timestamp").alias("timestamp"),
        F.col("parsed_amount_cents").alias("amount_cents"), "currency",
        F.year("parsed_timestamp").alias("year"), F.month("parsed_timestamp").alias("month"),
    )


def prepare_silver(transactions, customers, merchants):
    validate_reference(customers, "customer_id")
    validate_reference(merchants, "merchant_id")
    conflicts = find_conflicting_ids(transactions).cache()
    conflict_rows = transactions.join(conflicts, "transaction_id", "inner").select(
        *TRANSACTION_COLUMNS, F.array(F.lit("conflicting_transaction_id")).alias("rejection_reasons"))
    remaining = transactions.join(conflicts, "transaction_id", "left_anti")
    validated = validate_transactions(remaining, customers, merchants).cache()
    try:
        invalid = validated.filter(F.size("rejection_reasons") > 0).select(*TRANSACTION_COLUMNS, "rejection_reasons")
        valid = validated.filter(F.size("rejection_reasons") == 0)
        clean = deduplicate_transactions(valid)
        rejected = conflict_rows.unionByName(invalid)
        input_count = transactions.count()
        conflict_count = conflict_rows.count()
        invalid_count = invalid.count()
        valid_count = valid.count()
        final_count = clean.count()
        duplicates_removed = valid_count - final_count
        rejected_count = conflict_count + invalid_count
        accounted_count = rejected_count + duplicates_removed + final_count
        if input_count != accounted_count:
            raise RuntimeError(
                f"Transaction counts do not reconcile: input={input_count}, "
                f"rejected={rejected_count}, duplicates_removed={duplicates_removed}, "
                f"final={final_count}"
            )
        report = {
            "input_customers": customers.count(), "input_merchants": merchants.count(),
            "input_transactions": input_count,
            "validation_rejected_rows": invalid_count,
            "conflicting_transaction_ids": conflicts.count(),
            "conflict_rejected_rows": conflict_count,
            "rejected_rows": rejected_count,
            "exact_duplicates_removed": duplicates_removed,
            "final_transactions": final_count,
            "reconciliation": "input_transactions = rejected_rows + exact_duplicates_removed + final_transactions",
            "processing_order": ["reference_checks", "conflict_quarantine", "validation", "exact_deduplication"],
        }
        return clean, rejected, report
    finally:
        validated.unpersist()
        conflicts.unpersist()

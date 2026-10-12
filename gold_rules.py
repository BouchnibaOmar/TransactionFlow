"""Monthly aggregations with checked joins and currency reconciliation."""

from pyspark.sql import functions as F

from silver_rules import is_missing_identifier, validate_reference


def join_reference(transactions, reference, identifier, input_count):
    missing_references = transactions.join(reference.select(identifier), identifier, "left_anti")
    if missing_references.limit(1).count():
        raise ValueError(f"Silver transactions have missing references: {identifier}")
    joined = transactions.join(reference, identifier, "inner")
    joined_count = joined.count()
    if joined_count != input_count:
        raise RuntimeError(
            f"Join changed transaction count for {identifier}: "
            f"expected {input_count}, got {joined_count}"
        )
    return joined, joined_count


def aggregate_monthly(transactions, dimensions):
    return transactions.groupBy("year", "month", *dimensions, "currency").agg(
        F.count(F.lit(1)).alias("transaction_count"),
        F.sum("amount_cents").alias("total_amount_cents"),
    )


def currency_totals(frame, count_column=None, amount_column="amount_cents"):
    count_expression = F.count(F.lit(1)) if count_column is None else F.sum(count_column)
    # Decimal integers avoid overflow in checks spanning several monthly groups.
    totals = frame.groupBy("currency").agg(
        count_expression.alias("transaction_count"),
        F.sum(F.col(amount_column).cast("decimal(38,0)")).alias("total_amount_cents"),
    )
    return {
        row.currency: {
            "transaction_count": int(row.transaction_count),
            "total_amount_cents": int(row.total_amount_cents),
        }
        for row in totals.collect()
    }


def reconcile_monthly(monthly, expected_totals, table_name):
    actual_totals = currency_totals(monthly, "transaction_count", "total_amount_cents")
    if actual_totals != expected_totals:
        raise RuntimeError(
            f"Currency totals do not reconcile for {table_name}: "
            f"expected {expected_totals}, got {actual_totals}"
        )
    return {"rows": monthly.count(), "by_currency": actual_totals}


def prepare_gold(transactions, customers, merchants):
    validate_reference(customers, "customer_id")
    validate_reference(merchants, "merchant_id")
    input_count = transactions.count()
    if input_count == 0:
        raise ValueError("Silver input is empty; Gold requires at least one transaction")
    invalid_totals_input = (
        F.col("amount_cents").isNull() | is_missing_identifier(F.col("currency"))
    )
    if transactions.filter(invalid_totals_input).limit(1).count():
        raise ValueError("Silver amounts and currencies must not be missing")
    customer_transactions, customer_join_count = join_reference(
        transactions, customers, "customer_id", input_count
    )
    merchant_transactions, merchant_join_count = join_reference(
        transactions, merchants, "merchant_id", input_count
    )
    customer_monthly = aggregate_monthly(customer_transactions, ["customer_id", "country", "segment"])
    merchant_monthly = aggregate_monthly(merchant_transactions, ["merchant_id", "category"])
    expected_totals = currency_totals(transactions)
    report = {
        "input_transactions": input_count,
        "input_by_currency": expected_totals,
        "join_counts": {"customers": customer_join_count, "merchants": merchant_join_count},
        "checks": {
            "reference_identifiers_unique_and_nonempty": True,
            "transaction_references_present": True,
            "join_row_counts_preserved": True,
            "counts_and_amounts_reconciled_per_currency": True,
        },
        "merchant_monthly": reconcile_monthly(merchant_monthly, expected_totals, "merchant_monthly"),
        "customer_monthly": reconcile_monthly(customer_monthly, expected_totals, "customer_monthly"),
    }
    return merchant_monthly, customer_monthly, report

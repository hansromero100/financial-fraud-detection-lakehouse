import dlt
from pyspark.sql.functions import (
    col,
    when,
    sha2,
    current_timestamp,
    lit,
    regexp_replace,
)

BRONZE_TABLE = "fraud_detection_dev.bronze.transactions_raw"
SILVER_TABLE = "fraud_detection_dev.silver.transactions_clean"
VALID_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def anonymize_account_id(account_col):
    return sha2(regexp_replace(account_col, r"[CM]", ""), 256).alias(account_col)


def apply_dynamic_masking(df):
    df = df.withColumn("nameOrig_masked", sha2(regexp_replace("nameOrig", r"[CM]", ""), 256))
    df = df.withColumn("nameDest_masked", sha2(regexp_replace("nameDest", r"[CM]", ""), 256))
    df = df.drop("nameOrig", "nameDest")
    df = df.withColumnRenamed("nameOrig_masked", "nameOrig")
    df = df.withColumnRenamed("nameDest_masked", "nameDest")
    return df


def apply_cdc_merge(spark, silver_table, bronze_table):
    merge_sql = f"""
        MERGE INTO {silver_table} AS target
        USING {bronze_table} AS source
        ON target.step = source.step
           AND target.nameOrig = source.nameOrig
           AND target.nameDest = source.nameDest
           AND target.amount = source.amount
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)


@dlt.table(
    name="transactions_clean",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver",
    },
    comment="Silver layer: cleaned, anonymized, and validated transaction data with data quality constraints",
)
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect("valid_transaction_type", "type IN ('CASH_IN','CASH_OUT','DEBIT','PAYMENT','TRANSFER')")
@dlt.expect_or_drop("non_null_origin", "nameOrig IS NOT NULL")
@dlt.expect_or_drop("non_null_destination", "nameDest IS NOT NULL")
@dlt.expect_or_fail("valid_fraud_flag", "isFraud IN (0, 1)")
def silver_transactions():
    bronze_df = dlt.read("fraud_detection_dev.bronze.transactions_raw")

    cleaned = (
        bronze_df
        .filter(col("amount") > 0)
        .filter(col("type").isin(VALID_TYPES))
        .filter(col("nameOrig").isNotNull())
        .filter(col("nameDest").isNotNull())
        .transform(apply_dynamic_masking)
        .withColumn("transaction_date", col("ingestion_timestamp").cast("date"))
        .withColumn("is_high_value", when(col("amount") >= 10000, lit(True)).otherwise(lit(False)))
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("delta_operation", lit("INSERT"))
    )
    return cleaned

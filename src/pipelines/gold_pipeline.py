import dlt
from pyspark.sql.functions import (
    col,
    window,
    count,
    sum as _sum,
    avg as _avg,
    max as _max,
    lit,
    when,
    expr,
)

SILVER_TABLE = "fraud_detection_dev.silver.transactions_clean"
GOLD_VELOCITY_TABLE = "fraud_detection_dev.gold.fraud_velocity_metrics"
GOLD_REPORTING_TABLE = "fraud_detection_dev.gold.daily_fraud_report"
HIGH_VALUE_THRESHOLD = 10000.0
VELOCITY_WINDOW_HOURS = 1


@dlt.table(
    name="fraud_velocity_metrics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
    partition_cols=["transaction_date"],
    cluster_by=["transaction_type", "transaction_date"],
    comment="Gold layer: 1-hour sliding window velocity metrics for fraud scoring per user",
)
def gold_velocity_metrics():
    silver_df = dlt.read(SILVER_TABLE)

    velocity = (
        silver_df
        .withWatermark("processed_timestamp", "2 hours")
        .groupBy(
            col("nameOrig"),
            window(col("processed_timestamp"), f"{VELOCITY_WINDOW_HOURS} hour", "15 minutes"),
            col("type").alias("transaction_type"),
            col("transaction_date"),
        )
        .agg(
            count("*").alias("tx_count"),
            _sum("amount").alias("total_amount"),
            _avg("amount").alias("avg_amount"),
            _max("amount").alias("max_amount"),
            _sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("fraud_count"),
            _sum(when(col("amount") >= HIGH_VALUE_THRESHOLD, 1).otherwise(0)).alias("high_value_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn(
            "fraud_score",
            when(
                (col("high_value_count") >= 3) | (col("fraud_count") > 0),
                0.9,
            )
            .when(col("high_value_count") >= 2, 0.7)
            .when(col("high_value_count") >= 1, 0.4)
            .otherwise(0.1),
        )
        .withColumn(
            "is_suspicious",
            when(col("fraud_score") >= 0.7, lit(True)).otherwise(lit(False)),
        )
    )
    return velocity


@dlt.table(
    name="daily_fraud_report",
    table_properties={
        "quality": "gold",
    },
    partition_cols=["transaction_date"],
    cluster_by=["transaction_type", "transaction_date"],
    comment="Gold layer: daily aggregated fraud reporting table for BI dashboards",
)
def gold_daily_fraud_report():
    silver_df = dlt.read(SILVER_TABLE)

    report = (
        silver_df
        .groupBy(
            col("transaction_date"),
            col("type").alias("transaction_type"),
        )
        .agg(
            count("*").alias("total_transactions"),
            _sum("amount").alias("total_volume"),
            _avg("amount").alias("avg_transaction_amount"),
            _sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("fraud_transactions"),
            _sum(when(col("isFraud") == 0, 1).otherwise(0)).alias("legitimate_transactions"),
            _sum(when(col("isFraud") == 1, col("amount")).otherwise(lit(0))).alias("fraud_volume"),
            _sum(when(col("is_high_value") == True, 1).otherwise(0)).alias("high_value_transactions"),
            countDistinct("nameOrig").alias("unique_originators"),
            countDistinct("nameDest").alias("unique_recipients"),
        )
        .withColumn(
            "fraud_rate",
            expr("fraud_transactions / total_transactions"),
        )
        .withColumn(
            "fraud_volume_rate",
            expr("fraud_volume / total_volume"),
        )
    )
    return report

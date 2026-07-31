from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, count, sum as _sum, avg as _avg, max as _max, countDistinct, expr, window

from src.ingestion.auto_loader import BRONZE_SCHEMA, read_batch_csv
from src.pipelines.silver_pipeline import (
    apply_dynamic_masking,
    VALID_TYPES,
)
from src.pipelines.gold_pipeline import HIGH_VALUE_THRESHOLD

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "FraudDetectionLakehouse") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def run_bronze_to_delta(spark: SparkSession, source_path: str, table_name: str) -> None:
    df = read_batch_csv(spark, source_path)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    logger.info("Bronze table '%s' loaded with %d records", table_name, df.count())


def run_silver_transform(spark: SparkSession, bronze_table: str, silver_table: str) -> DataFrame:
    bronze_df = spark.table(bronze_table)

    cleaned = (
        bronze_df
        .filter(col("amount") > 0)
        .filter(col("type").isin(VALID_TYPES))
        .filter(col("nameOrig").isNotNull())
        .filter(col("nameDest").isNotNull())
        .transform(apply_dynamic_masking)
        .withColumn("transaction_date", col("ingestion_timestamp").cast("date"))
        .withColumn("is_high_value", when(col("amount") >= HIGH_VALUE_THRESHOLD, lit(True)).otherwise(lit(False)))
    )
    cleaned.write.format("delta").mode("overwrite").saveAsTable(silver_table)
    logger.info("Silver table '%s' loaded with %d records", silver_table, cleaned.count())
    return cleaned


def run_gold_aggregation(spark: SparkSession, silver_table: str, gold_table: str) -> DataFrame:
    silver_df = spark.table(silver_table)

    report = (
        silver_df
        .groupBy("transaction_date", col("type").alias("transaction_type"))
        .agg(
            count("*").alias("total_transactions"),
            _sum("amount").alias("total_volume"),
            _avg("amount").alias("avg_transaction_amount"),
            _sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("fraud_transactions"),
            _sum(when(col("is_high_value") == True, 1).otherwise(0)).alias("high_value_transactions"),
            countDistinct("nameOrig").alias("unique_originators"),
        )
        .withColumn("fraud_rate", expr("fraud_transactions / total_transactions"))
    )
    report.write.format("delta").mode("overwrite").saveAsTable(gold_table)
    logger.info("Gold table '%s' loaded with %d records", gold_table, report.count())
    return report

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])

VALID_TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def read_streaming_csv(
    spark: SparkSession,
    source_path: str,
    checkpoint_path: str,
    max_files_per_trigger: int = 1,
    schema: Optional[StructType] = None,
) -> DataFrame:
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("maxFilesPerTrigger", str(max_files_per_trigger))
        .schema(schema if schema else BRONZE_SCHEMA)
        .load(source_path)
    )
    logger.info("Created streaming reader for source: %s", source_path)
    return reader


def read_batch_csv(
    spark: SparkSession,
    source_path: str,
    schema: Optional[StructType] = None,
) -> DataFrame:
    reader = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema if schema else BRONZE_SCHEMA)
        .load(source_path)
    )
    logger.info("Created batch reader for source: %s", source_path)
    return reader

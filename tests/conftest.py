from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
    BooleanType,
)
import pytest


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("fraud_detection_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


SAMPLE_SCHEMA = StructType([
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

SILVER_SCHEMA = StructType([
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
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("is_high_value", BooleanType(), True),
])

SAMPLE_DATA = [
    (1, "TRANSFER", 325689.45, "C1234567", 2100000.00, 1774310.55, "M9876543", 500000.00, 825689.45, 0, 1),
    (2, "PAYMENT", 45230.10, "C2345678", 800000.00, 754769.90, "M1111111", 300000.00, 345230.10, 0, 0),
    (3, "TRANSFER", 5000000.00, "C3456789", 10000000.00, 5000000.00, "C4567890", 2000000.00, 7000000.00, 1, 1),
    (4, "CASH_OUT", 0.00, "C5678901", 500000.00, 500000.00, "C6789012", 1500000.00, 1500000.00, 0, 0),
    (5, "INVALID_TYPE", 10000.00, "C7890123", 400000.00, 390000.00, "M2222222", 600000.00, 610000.00, 0, 0),
    (6, "TRANSFER", -5000.00, "C8901234", 2500000.00, 2505000.00, "C9012345", 900000.00, 895000.00, 0, 0),
    (7, None, 75000.00, None, 1750000.00, 1675000.00, None, 750000.00, 825000.00, 0, 0),
    (8, "TRANSFER", 3250000.00, "C0123456", 5000000.00, 1750000.00, "C1122334", 1500000.00, 4750000.00, 1, 0),
]

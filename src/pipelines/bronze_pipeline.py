import dlt
from pyspark.sql.functions import col, current_timestamp, lit

from src.ingestion.auto_loader import BRONZE_SCHEMA

SOURCE_PATH = spark.conf.get("pipeline.source_path", "/tmp/paysim_landing")
CHECKPOINT_PATH = spark.conf.get("pipeline.checkpoint_path", "/tmp/checkpoints/bronze")


@dlt.table(
    name="transactions_raw",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze",
    },
    comment="Bronze layer: raw PaySim financial transaction data ingested via Auto Loader",
)
def bronze_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(BRONZE_SCHEMA)
        .option("maxFilesPerTrigger", "1")
        .load(SOURCE_PATH)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("paysim_synthetic"))
        .withColumn("isFraud", col("isFraud").cast("int"))
        .withColumn("isFlaggedFraud", col("isFlaggedFraud").cast("int"))
    )

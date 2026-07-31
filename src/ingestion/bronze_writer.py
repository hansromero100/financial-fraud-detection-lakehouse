import logging

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


def write_to_bronze(
    df: DataFrame,
    table_name: str,
    checkpoint_path: str,
    output_mode: str = "append",
    trigger: str = "ProcessingTime",
    trigger_interval: str = "10 seconds",
) -> None:
    query = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .toTable(table_name)
    )
    logger.info(
        "Writing to Bronze table: %s (trigger: %s %s, output_mode: %s)",
        table_name, trigger, trigger_interval, output_mode,
    )
    return query


def write_bronze_batch(df: DataFrame, table_name: str, mode: str = "append") -> None:
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    logger.info("Wrote batch to Bronze table: %s", table_name)

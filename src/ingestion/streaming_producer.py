import logging
import os
import shutil
import time
from typing import Optional

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

VALID_TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def generate_transaction_batches(
    csv_path: str,
    landing_dir: str,
    batch_size: int = 100,
    delay_seconds: float = 2.0,
    max_batches: Optional[int] = None,
) -> None:
    with open(csv_path, "r") as f:
        header = f.readline().strip()
        rows = f.readlines()

    os.makedirs(landing_dir, exist_ok=True)

    total_batches = min(len(rows) // batch_size + (1 if len(rows) % batch_size else 0), max_batches or 999999)

    for batch_idx in range(total_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(rows))
        batch_rows = rows[start:end]
        if not batch_rows:
            break

        filename = f"transactions_batch_{int(time.time())}_{batch_idx}.csv"
        filepath = os.path.join(landing_dir, filename)
        with open(filepath, "w") as bf:
            bf.write(header + "\n")
            bf.writelines(batch_rows)

        logger.info("Wrote batch %d/%d (%d rows) -> %s", batch_idx + 1, total_batches, len(batch_rows), filename)
        if batch_idx < total_batches - 1:
            time.sleep(delay_seconds)

    logger.info("Finished generating %d batches", total_batches)


def simulate_streaming_from_dataframe(
    spark: SparkSession,
    df: DataFrame,
    landing_dir: str,
    batch_size: int = 50,
    delay_seconds: float = 1.0,
) -> None:
    os.makedirs(landing_dir, exist_ok=True)
    pandas_df = df.toPandas()

    total_rows = len(pandas_df)
    num_batches = total_rows // batch_size + (1 if total_rows % batch_size else 0)

    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, total_rows)
        batch = pandas_df.iloc[start:end]

        filename = f"stream_batch_{int(time.time())}_{i}.csv"
        filepath = os.path.join(landing_dir, filename)
        batch.to_csv(filepath, index=False)

        logger.info("Streamed batch %d/%d -> %s", i + 1, num_batches, filename)
        if i < num_batches - 1:
            time.sleep(delay_seconds)


def cleanup_landing_dir(landing_dir: str) -> int:
    if not os.path.exists(landing_dir):
        return 0
    count = len([f for f in os.listdir(landing_dir) if f.endswith(".csv")])
    shutil.rmtree(landing_dir, ignore_errors=True)
    logger.info("Cleaned up %d files from %s", count, landing_dir)
    return count

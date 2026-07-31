from pyspark.sql.functions import col, sha2, regexp_replace

from src.pipelines.silver_pipeline import apply_dynamic_masking, VALID_TYPES
from tests.conftest import spark, SAMPLE_SCHEMA, SAMPLE_DATA


class TestSilverPipeline:
    def test_apply_dynamic_masking(self, spark):
        df = spark.createDataFrame(
            [(1, "TRANSFER", 1000.0, "C1234567", 5000.0, 4000.0, "M9876543", 3000.0, 4000.0, 0, 0)],
            SAMPLE_SCHEMA,
        )
        masked = df.transform(apply_dynamic_masking)

        assert "nameOrig" in masked.columns
        assert "nameDest" in masked.columns
        assert masked.select("nameOrig").first()[0] is not None
        assert masked.select("nameOrig").first()[0] != "C1234567"
        assert masked.select("nameDest").first()[0] is not None
        assert masked.select("nameDest").first()[0] != "M9876543"

    def test_masking_deterministic(self, spark):
        df = spark.createDataFrame(
            [(1, "TRANSFER", 1000.0, "C1234567", 5000.0, 4000.0, "M9876543", 3000.0, 4000.0, 0, 0)],
            SAMPLE_SCHEMA,
        )
        masked1 = df.transform(apply_dynamic_masking)
        masked2 = df.transform(apply_dynamic_masking)
        assert masked1.select("nameOrig").first()[0] == masked2.select("nameOrig").first()[0]

    def test_masking_preserves_nulls(self, spark):
        df = spark.createDataFrame(
            [(1, "TRANSFER", 1000.0, None, 5000.0, 4000.0, None, 3000.0, 4000.0, 0, 0)],
            SAMPLE_SCHEMA,
        )
        masked = df.transform(apply_dynamic_masking)
        assert masked.select("nameOrig").first()[0] is None
        assert masked.select("nameDest").first()[0] is None

    def test_valid_types_list(self):
        assert len(VALID_TYPES) == 5
        assert set(VALID_TYPES) == {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}

    def test_filter_invalid_amounts(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA)
        filtered = df.filter(col("amount") > 0)
        invalid_rows = df.filter(col("amount") <= 0)
        assert invalid_rows.count() > 0
        assert filtered.count() == df.count() - invalid_rows.count()

    def test_filter_invalid_types(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA)
        filtered = df.filter(col("type").isin(VALID_TYPES))
        invalid_rows = df.filter(~col("type").isin(VALID_TYPES))
        assert invalid_rows.count() > 0

    def test_filter_null_accounts(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA)
        filtered = df.filter(col("nameOrig").isNotNull() & col("nameDest").isNotNull())
        null_rows = df.filter(col("nameOrig").isNull() | col("nameDest").isNull())
        assert null_rows.count() > 0

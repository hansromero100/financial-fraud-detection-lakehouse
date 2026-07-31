from src.quality.expectations import load_expectations, apply_expectations
from tests.conftest import spark, SAMPLE_SCHEMA, SAMPLE_DATA


class TestQualityExpectations:
    def test_load_expectations(self):
        expectations = load_expectations("src/quality/expectations_silver.yaml")
        assert len(expectations) == 8
        names = [e["name"] for e in expectations]
        assert "valid_amount" in names
        assert "valid_transaction_type" in names
        assert "non_null_origin" in names
        assert "non_null_destination" in names
        assert "valid_fraud_flag" in names
        assert "valid_balance_non_negative" in names

    def test_apply_drop_expectations(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA)
        expectations = load_expectations("src/quality/expectations_silver.yaml")
        drop_expectations = [e for e in expectations if e.get("on_violation") == "drop"]
        result = apply_expectations(df, drop_expectations)
        assert result.count() < df.count()

    def test_apply_warn_expectations(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA)
        expectations = load_expectations("src/quality/expectations_silver.yaml")
        warn_expectations = [e for e in expectations if e.get("on_violation") == "warn"]
        result = apply_expectations(df, warn_expectations)
        assert result.count() == df.count()

    def test_expect_fail_on_invalid_fraud_flag(self, spark):
        invalid_data = [
            (1, "TRANSFER", 1000.0, "C1234567", 5000.0, 4000.0, "M9876543", 3000.0, 4000.0, 2, 0),
        ]
        df = spark.createDataFrame(invalid_data, SAMPLE_SCHEMA)
        expectations = load_expectations("src/quality/expectations_silver.yaml")
        fail_exp = [e for e in expectations if e["name"] == "valid_fraud_flag"]
        try:
            apply_expectations(df, fail_exp)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "valid_fraud_flag" in str(e)

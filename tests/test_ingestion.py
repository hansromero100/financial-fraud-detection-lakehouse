from src.ingestion.auto_loader import read_batch_csv, BRONZE_SCHEMA, VALID_TRANSACTION_TYPES
from tests.conftest import SAMPLE_SCHEMA, SAMPLE_DATA


class TestAutoLoader:
    def test_bronze_schema_fields(self):
        field_names = {f.name for f in BRONZE_SCHEMA.fields}
        expected = {
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest",
            "newbalanceDest", "isFraud", "isFlaggedFraud",
        }
        assert field_names == expected

    def test_bronze_schema_field_count(self):
        assert len(BRONZE_SCHEMA.fields) == 11

    def test_valid_transaction_types(self):
        assert "TRANSFER" in VALID_TRANSACTION_TYPES
        assert "CASH_OUT" in VALID_TRANSACTION_TYPES
        assert "PAYMENT" in VALID_TRANSACTION_TYPES
        assert "CASH_IN" in VALID_TRANSACTION_TYPES
        assert "DEBIT" in VALID_TRANSACTION_TYPES
        assert len(VALID_TRANSACTION_TYPES) == 5

    def test_read_batch_csv(self, spark, tmp_path):
        csv_path = tmp_path / "test_data.csv"
        import csv
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["step", "type", "amount", "nameOrig", "oldbalanceOrg",
                             "newbalanceOrig", "nameDest", "oldbalanceDest",
                             "newbalanceDest", "isFraud", "isFlaggedFraud"])
            writer.writerow([1, "TRANSFER", 325689.45, "C1234567", 2100000.00,
                             1774310.55, "M9876543", 500000.00, 825689.45, 0, 1])

        df = read_batch_csv(spark, str(csv_path))
        assert df.count() == 1
        assert df.columns == [
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest",
            "newbalanceDest", "isFraud", "isFlaggedFraud",
        ]

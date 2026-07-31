from src.ingestion.streaming_producer import generate_transaction_batches, cleanup_landing_dir
from tests.conftest import SAMPLE_SCHEMA, SAMPLE_DATA
import os


class TestStreamingProducer:
    def test_generate_transaction_batches(self, tmp_path):
        landing_dir = str(tmp_path / "landing")
        generate_transaction_batches(
            csv_path="data/sample_paysim.csv",
            landing_dir=landing_dir,
            batch_size=3,
            delay_seconds=0,
        )
        csv_files = [f for f in os.listdir(landing_dir) if f.endswith(".csv")]
        assert len(csv_files) > 0

    def test_cleanup_landing_dir(self, tmp_path):
        landing_dir = str(tmp_path / "landing")
        os.makedirs(landing_dir, exist_ok=True)
        with open(os.path.join(landing_dir, "test.csv"), "w") as f:
            f.write("test")
        count = cleanup_landing_dir(landing_dir)
        assert count == 1
        assert not os.path.exists(landing_dir)

    def test_cleanup_nonexistent_dir(self):
        count = cleanup_landing_dir("/tmp/nonexistent_dir_for_test")
        assert count == 0

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_bronze_schema(schema_path: str) -> dict:
    with open(schema_path, "r") as f:
        schema_json = json.load(f)
    logger.info("Loaded bronze schema from %s", schema_path)
    return schema_json


def get_schema_path(config_dir: str = "config/schemas") -> str:
    return str(Path(__file__).resolve().parents[2] / config_dir / "bronze_schema.json")

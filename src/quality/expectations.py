import yaml
import logging
from pathlib import Path
from typing import List, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def load_expectations(yaml_path: str) -> List[Dict[str, Any]]:
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    expectations = config.get("expectations", [])
    logger.info("Loaded %d expectations from %s", len(expectations), yaml_path)
    return expectations


def apply_expectations(df: DataFrame, expectations: List[Dict[str, Any]]) -> DataFrame:
    filtered = df
    for exp in expectations:
        name = exp["name"]
        condition = exp["condition"]
        action = exp.get("on_violation", "warn")

        if action == "drop":
            before_count = filtered.count()
            filtered = filtered.filter(condition)
            dropped = before_count - filtered.count()
            if dropped > 0:
                logger.warning(
                    "Expectation '%s': dropped %d rows (condition: %s)",
                    name, dropped, condition,
                )
        elif action == "fail":
            violations = filtered.filter(f"NOT ({condition})")
            if violations.count() > 0:
                raise ValueError(
                    f"Expectation '{name}' failed: {violations.count()} rows violate: {condition}"
                )
        else:
            logger.info("Expectation '%s' (warn): applied condition %s", name, condition)

    return filtered


def get_expectations_path(config_dir: str = "src/quality") -> str:
    return str(Path(__file__).resolve().parent / "expectations_silver.yaml")

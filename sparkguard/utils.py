"""Utility helpers for navigating Spark submit JSON configs."""

from __future__ import annotations

from typing import Any


def get_spark_conf(config: dict) -> dict[str, Any]:
    """Extract the Spark configuration map from various config shapes.

    Handles:
      - {"conf": {"spark.foo": ...}}           (raw conf block)
      - {"sparkConf": {"spark.foo": ...}}       (Livy-style)
      - {"spark.foo": ...}                      (flat top-level keys)
      - Nested under arbitrary wrapper keys that contain a conf/sparkConf child
    """
    # Direct keys
    for key in ("conf", "sparkConf", "spark_conf"):
        if key in config and isinstance(config[key], dict):
            return config[key]

    # Flat — if top-level keys start with "spark."
    if any(str(k).startswith("spark.") for k in config):
        return {k: v for k, v in config.items() if str(k).startswith("spark.")}

    # One-level deep search
    for _key, value in config.items():
        if isinstance(value, dict):
            for sub in ("conf", "sparkConf", "spark_conf"):
                if sub in value and isinstance(value[sub], dict):
                    return value[sub]

    return {}


def parse_memory(value: str | int | float) -> int | None:
    """Parse a Spark memory string (e.g. '4g', '512m') into megabytes."""
    if isinstance(value, (int, float)):
        return int(value)
    value = str(value).strip().lower()
    try:
        if value.endswith("g"):
            return int(float(value[:-1]) * 1024)
        if value.endswith("m"):
            return int(float(value[:-1]))
        if value.endswith("k"):
            return max(1, int(float(value[:-1]) / 1024))
        if value.endswith("t"):
            return int(float(value[:-1]) * 1024 * 1024)
        return int(value)
    except (ValueError, TypeError):
        return None


def as_bool(value: Any) -> bool | None:
    """Coerce a config value to a boolean, returning None if unparseable."""
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None

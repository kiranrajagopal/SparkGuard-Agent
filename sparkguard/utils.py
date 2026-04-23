"""Utility helpers for navigating Spark submit JSON configs."""

from __future__ import annotations

from typing import Any


def get_spark_conf(config: dict) -> dict[str, Any]:
    """Extract the Spark configuration map from various config shapes.

    Handles:
      - {"conf": {"spark.foo": ...}}              (raw conf block)
      - {"sparkConf": {"spark.foo": ...}}          (Livy-style)
      - {"spark_conf": {"spark.foo": ...}}         (Databricks / snake_case)
      - {"spark.foo": ...}                         (flat top-level keys)
      - {"spec": {"sparkConf": {...}}}             (K8s SparkApplication operator)
      - {"task": {"spark_submit": {"conf": {...}}}} (Airflow, arbitrary depth)
      - {"spark": {"executor": {"memory": "8g"}}}  (nested YAML/HOCON style)
      - [{"Key": "spark.foo", "Value": "bar"}]     (EMR-style key-value list, via parent dict)
    """
    _CONF_KEYS = ("conf", "sparkConf", "spark_conf")

    # 1. Direct conf/sparkConf/spark_conf at top level
    for key in _CONF_KEYS:
        if key in config and isinstance(config[key], dict):
            return config[key]

    # 2. Flat — top-level keys start with "spark."
    if any(str(k).startswith("spark.") for k in config):
        return {k: v for k, v in config.items() if str(k).startswith("spark.")}

    # 3. Deep search — find conf/sparkConf/spark_conf at any depth
    found = _deep_find_conf(config, _CONF_KEYS, max_depth=6)
    if found is not None:
        return found

    # 4. Nested YAML/HOCON: {"spark": {"executor": {"memory": "8g"}}}
    #    Flatten into dotted keys
    if "spark" in config and isinstance(config["spark"], dict):
        return _flatten_to_spark_conf(config["spark"], prefix="spark")

    # 5. EMR-style key-value list: look for lists of dicts with Key/Value or Name/Value
    for _key, value in config.items():
        if isinstance(value, list):
            extracted = _extract_kv_list(value)
            if extracted:
                return extracted
        # One level deeper
        if isinstance(value, dict):
            for _subkey, subval in value.items():
                if isinstance(subval, list):
                    extracted = _extract_kv_list(subval)
                    if extracted:
                        return extracted

    return {}


def _deep_find_conf(
    obj: dict, conf_keys: tuple[str, ...], max_depth: int, _depth: int = 0,
) -> dict | None:
    """Recursively search for a conf/sparkConf/spark_conf dict up to max_depth."""
    if _depth > max_depth:
        return None
    for key, value in obj.items():
        if key in conf_keys and isinstance(value, dict):
            return value
        if isinstance(value, dict):
            result = _deep_find_conf(value, conf_keys, max_depth, _depth + 1)
            if result is not None:
                return result
    return None


def _flatten_to_spark_conf(d: dict, prefix: str) -> dict[str, Any]:
    """Flatten nested dict into dotted spark.* keys.

    {"executor": {"memory": "8g"}} with prefix="spark"
    → {"spark.executor.memory": "8g"}
    """
    result: dict[str, Any] = {}
    for key, value in d.items():
        full_key = f"{prefix}.{key}"
        if isinstance(value, dict):
            result.update(_flatten_to_spark_conf(value, full_key))
        else:
            result[full_key] = value
    return result


def _extract_kv_list(items: list) -> dict[str, Any]:
    """Extract spark.* keys from EMR/Dataproc-style [{Key: ..., Value: ...}] lists."""
    result: dict[str, Any] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        # Try common key-value patterns
        for k_field, v_field in [("Key", "Value"), ("key", "value"), ("Name", "Value"), ("name", "value")]:
            if k_field in item and v_field in item:
                key = str(item[k_field])
                if key.startswith("spark."):
                    result[key] = item[v_field]
                break
    return result


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

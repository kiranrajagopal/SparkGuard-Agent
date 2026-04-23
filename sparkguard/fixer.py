"""Auto-fixer: applies safe, mechanical fixes to Spark configs."""

from __future__ import annotations

import json
from typing import Any

from sparkguard.models import Finding, Severity
from sparkguard.parser import parse_spark_config
from sparkguard.utils import get_spark_conf, parse_memory


# Maps rule id → fixer function(conf_dict, finding) → (was_fixed, description)
FixerFunc = type(lambda conf, finding: (False, ""))

_FIXERS: dict[str, Any] = {}


def fixer(rule_id: str):
    """Register an auto-fix function for a rule."""
    def decorator(fn):
        _FIXERS[rule_id] = fn
        return fn
    return decorator


def fix_config(raw: str, filename: str = "") -> tuple[str, list[str]]:
    """Apply all safe auto-fixes to a config in any supported format.

    Returns (fixed_output, list_of_descriptions_of_what_was_fixed).
    Output is always JSON (regardless of input format).
    """
    from sparkguard.linter import lint
    from sparkguard.formats import parse_config

    parsed, parse_findings = parse_config(raw, filename=filename)
    conf = get_spark_conf(parsed)
    report = lint(raw, config_path=filename)

    descriptions: list[str] = []

    # Handle duplicate keys: rebuild from parsed (already deduplicated via last-wins)
    dup_findings = [f for f in parse_findings if f.rule == "duplicate-key"]
    if dup_findings:
        for f in dup_findings:
            descriptions.append(f"Removed duplicate key '{f.keys[0]}' (kept last value)")

    # Apply rule-specific fixers
    for finding in report.findings:
        if finding.rule in _FIXERS:
            fixed, desc = _FIXERS[finding.rule](conf, finding)
            if fixed:
                descriptions.append(desc)

    # Rebuild the output JSON
    # Preserve top-level structure, update the conf block
    _set_spark_conf(parsed, conf)
    fixed_json = json.dumps(parsed, indent=4)

    return fixed_json, descriptions


def _set_spark_conf(config: dict, new_conf: dict) -> None:
    """Write the spark conf back into the config dict, preserving structure."""
    for key in ("conf", "sparkConf", "spark_conf"):
        if key in config and isinstance(config[key], dict):
            config[key] = new_conf
            return
    # Flat keys
    spark_keys = [k for k in config if str(k).startswith("spark.")]
    if spark_keys:
        for k in spark_keys:
            del config[k]
        config.update(new_conf)
        return
    # Nested one level
    for _key, value in config.items():
        if isinstance(value, dict):
            for sub in ("conf", "sparkConf", "spark_conf"):
                if sub in value and isinstance(value[sub], dict):
                    value[sub] = new_conf
                    return
    # Fallback: create a conf key
    config["conf"] = new_conf


# ---------------------------------------------------------------------------
# Fixers for specific rules
# ---------------------------------------------------------------------------

@fixer("aqe-contradictions")
def _fix_aqe_contradictions(conf: dict, finding: Finding) -> tuple[bool, str]:
    """If AQE is off but sub-settings are present, enable AQE."""
    conf["spark.sql.adaptive.enabled"] = "true"
    return True, "Enabled AQE (spark.sql.adaptive.enabled=true) to match existing sub-settings"


@fixer("missing-spark35-recommendations")
def _fix_missing_recommendations(conf: dict, finding: Finding) -> tuple[bool, str]:
    """Add missing recommended settings."""
    key = finding.keys[0]
    defaults = {
        "spark.sql.adaptive.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    }
    if key in defaults and key not in conf:
        conf[key] = defaults[key]
        return True, f"Added recommended setting {key}={defaults[key]}"
    return False, ""


@fixer("low-memory-overhead")
def _fix_low_overhead(conf: dict, finding: Finding) -> tuple[bool, str]:
    """Bump memoryOverhead to 10% of executorMemory."""
    exec_mem_str = conf.get("spark.executor.memory")
    if exec_mem_str is None:
        return False, ""
    exec_mem = parse_memory(exec_mem_str)
    if exec_mem is None:
        return False, ""
    recommended = max(384, int(exec_mem * 0.10))
    conf["spark.executor.memoryOverhead"] = f"{recommended}m"
    # Also update the legacy key if present
    if "spark.yarn.executor.memoryOverhead" in conf:
        conf["spark.yarn.executor.memoryOverhead"] = f"{recommended}m"
    return True, f"Set spark.executor.memoryOverhead to {recommended}m (10% of executorMemory)"


@fixer("offheap-missing-size")
def _fix_offheap(conf: dict, finding: Finding) -> tuple[bool, str]:
    """If off-heap enabled without size, disable off-heap (safe default)."""
    conf["spark.memory.offHeap.enabled"] = "false"
    return True, "Disabled off-heap memory (was enabled without a size — safer to disable)"


@fixer("heartbeat-timeout-mismatch")
def _fix_heartbeat(conf: dict, finding: Finding) -> tuple[bool, str]:
    """Set heartbeat to 1/3 of network timeout."""
    timeout_str = conf.get("spark.network.timeout")
    if timeout_str is None:
        return False, ""

    def _parse_seconds(val: str) -> int | None:
        val = str(val).strip().lower()
        try:
            if val.endswith("ms"):
                return int(float(val[:-2]) / 1000)
            if val.endswith("s"):
                return int(float(val[:-1]))
            if val.endswith("m") or val.endswith("min"):
                return int(float(val.rstrip("min")) * 60)
            return int(val)
        except (ValueError, TypeError):
            return None

    timeout_s = _parse_seconds(timeout_str)
    if timeout_s is None:
        return False, ""
    new_hb = max(10, timeout_s // 3)
    conf["spark.executor.heartbeatInterval"] = f"{new_hb}s"
    return True, f"Set heartbeatInterval to {new_hb}s (1/3 of network.timeout={timeout_str})"


@fixer("shuffle-partitions-type")
def _fix_shuffle_type(conf: dict, finding: Finding) -> tuple[bool, str]:
    """Coerce shuffle.partitions to a clean integer string."""
    val = conf.get("spark.sql.shuffle.partitions")
    if val is None:
        return False, ""
    try:
        clean = str(int(float(str(val).strip())))
        conf["spark.sql.shuffle.partitions"] = clean
        return True, f"Coerced shuffle.partitions from '{val}' to '{clean}'"
    except (ValueError, TypeError):
        return False, ""

"""Tests for sparkguard fix (auto-fixer)."""

import json
import pytest

from sparkguard.fixer import fix_config
from sparkguard.linter import lint
from sparkguard.models import Severity


def _make_conf(**spark_keys) -> str:
    return json.dumps({"conf": spark_keys})


class TestFixDuplicateKeys:
    def test_removes_duplicate_key(self):
        raw = """{
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.executor.memory": "8g",
                "spark.sql.adaptive.enabled": "false"
            }
        }"""
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        # Duplicate is removed (parsed dict only has one key).
        # No sub-settings present, so AQE fixer doesn't trigger — value stays as last-wins.
        assert "spark.sql.adaptive.enabled" in fixed["conf"]
        assert any("duplicate" in d.lower() for d in descriptions)

    def test_removes_duplicate_and_fixes_aqe_contradiction(self):
        """Duplicate key + AQE sub-settings → fixer enables AQE."""
        raw = """{
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.executor.memory": "8g",
                "spark.sql.adaptive.enabled": "false"
            }
        }"""
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        assert fixed["conf"]["spark.sql.adaptive.enabled"] == "true"
        assert any("duplicate" in d.lower() for d in descriptions)
        assert any("AQE" in d or "aqe" in d.lower() for d in descriptions)


class TestFixAQEContradictions:
    def test_enables_aqe_when_sub_settings_present(self):
        raw = _make_conf(**{
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        })
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        assert fixed["conf"]["spark.sql.adaptive.enabled"] == "true"
        assert any("AQE" in d or "aqe" in d.lower() for d in descriptions)


class TestFixMissingRecommendations:
    def test_adds_kryo_serializer(self):
        raw = _make_conf(**{"spark.executor.memory": "4g"})
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        assert fixed["conf"].get("spark.serializer") == "org.apache.spark.serializer.KryoSerializer"


class TestFixLowOverhead:
    def test_bumps_low_overhead(self):
        raw = _make_conf(**{
            "spark.executor.memory": "16g",
            "spark.executor.memoryOverhead": "256m",
        })
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        overhead_str = fixed["conf"]["spark.executor.memoryOverhead"]
        overhead_mb = int(overhead_str.rstrip("m"))
        assert overhead_mb >= 1638  # 10% of 16g


class TestFixOffheap:
    def test_disables_offheap_without_size(self):
        raw = _make_conf(**{"spark.memory.offHeap.enabled": "true"})
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        assert fixed["conf"]["spark.memory.offHeap.enabled"] == "false"


class TestFixHeartbeat:
    def test_fixes_heartbeat_ratio(self):
        raw = _make_conf(**{
            "spark.executor.heartbeatInterval": "120s",
            "spark.network.timeout": "120s",
        })
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        assert fixed["conf"]["spark.executor.heartbeatInterval"] == "40s"


class TestFixShuffleType:
    def test_coerces_float_to_int(self):
        raw = _make_conf(**{"spark.sql.shuffle.partitions": "200.0"})
        fixed_json, descriptions = fix_config(raw)
        fixed = json.loads(fixed_json)
        assert fixed["conf"]["spark.sql.shuffle.partitions"] == "200"


class TestFixIdempotent:
    def test_fix_then_lint_has_no_errors(self):
        """The fixed output of a bad config should have zero ERROR-level findings."""
        raw = """{
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.executor.memory": "16g",
                "spark.executor.memoryOverhead": "256m",
                "spark.memory.offHeap.enabled": "true",
                "spark.executor.heartbeatInterval": "120s",
                "spark.network.timeout": "120s",
                "spark.sql.shuffle.partitions": "200.0",
                "spark.sql.adaptive.enabled": "false",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }"""
        fixed_json, _ = fix_config(raw)
        report = lint(fixed_json)
        errors = [f for f in report.findings if f.severity == Severity.ERROR]
        assert errors == [], f"Fix didn't resolve: {[e.message for e in errors]}"

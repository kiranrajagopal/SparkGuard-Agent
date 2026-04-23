"""Tests for duplicate-key JSON parser."""

import pytest

from sparkguard.parser import parse_spark_config
from sparkguard.models import Severity


class TestDuplicateKeys:
    def test_no_duplicates(self):
        raw = '{"conf": {"spark.executor.memory": "4g"}}'
        parsed, findings = parse_spark_config(raw)
        assert findings == []
        assert parsed["conf"]["spark.executor.memory"] == "4g"

    def test_duplicate_different_values_is_error(self):
        """The exact bug: AQE true overridden by AQE false via duplicate key."""
        raw = """{
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.executor.memory": "8g",
                "spark.sql.adaptive.enabled": "false"
            }
        }"""
        parsed, findings = parse_spark_config(raw)
        assert len(findings) == 1
        f = findings[0]
        assert f.severity == Severity.ERROR
        assert f.rule == "duplicate-key"
        assert "spark.sql.adaptive.enabled" in f.keys
        # Parsed config should reflect last-wins (what Spark would actually see)
        assert parsed["conf"]["spark.sql.adaptive.enabled"] == "false"

    def test_duplicate_same_value_is_warn(self):
        raw = """{
            "conf": {
                "spark.executor.memory": "4g",
                "spark.executor.memory": "4g"
            }
        }"""
        _, findings = parse_spark_config(raw)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARN

    def test_multiple_duplicates(self):
        raw = """{
            "conf": {
                "spark.a": "1",
                "spark.a": "2",
                "spark.b": "x",
                "spark.b": "y"
            }
        }"""
        _, findings = parse_spark_config(raw)
        assert len(findings) == 2

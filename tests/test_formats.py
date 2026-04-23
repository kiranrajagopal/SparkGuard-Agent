"""Tests for multi-format config parsing."""

from __future__ import annotations

import json

import pytest

from sparkguard.formats import detect_format, parse_config, _parse_properties, _flatten_dict
from sparkguard.linter import lint
from sparkguard.models import Severity


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

class TestDetectFormat:
    def test_json_from_extension(self):
        assert detect_format("{}", "config.json") == "json"

    def test_yaml_from_extension(self):
        assert detect_format("", "config.yaml") == "yaml"
        assert detect_format("", "config.yml") == "yaml"

    def test_properties_from_extension(self):
        assert detect_format("", "spark-defaults.conf") == "properties"
        assert detect_format("", "config.properties") == "properties"

    def test_hocon_from_extension(self):
        assert detect_format("", "config.hocon") == "hocon"

    def test_json_from_content(self):
        assert detect_format('{"spark.foo": "bar"}') == "json"

    def test_properties_from_content(self):
        content = "spark.executor.memory  8g\nspark.driver.memory  4g\n"
        assert detect_format(content) == "properties"

    def test_yaml_from_content(self):
        content = "spark:\n  executor:\n    memory: 8g\n"
        assert detect_format(content) == "yaml"

    def test_hocon_from_content(self):
        content = 'include "base.conf"\nspark { executor { memory = 8g } }'
        assert detect_format(content) == "hocon"

    def test_hocon_substitution(self):
        content = "spark.executor.memory = ${MEM}"
        assert detect_format(content) == "hocon"


# ---------------------------------------------------------------------------
# Properties parsing
# ---------------------------------------------------------------------------

class TestPropertiesParser:
    def test_basic_properties(self):
        raw = "spark.executor.memory  8g\nspark.driver.memory=4g\n"
        parsed, findings = _parse_properties(raw)
        assert parsed["spark.executor.memory"] == "8g"
        assert parsed["spark.driver.memory"] == "4g"
        assert len(findings) == 0

    def test_comments_ignored(self):
        raw = "# this is a comment\nspark.executor.memory  8g\n! another comment\n"
        parsed, findings = _parse_properties(raw)
        assert len(parsed) == 1
        assert parsed["spark.executor.memory"] == "8g"

    def test_empty_lines_ignored(self):
        raw = "\n\nspark.executor.memory  8g\n\n"
        parsed, findings = _parse_properties(raw)
        assert len(parsed) == 1

    def test_duplicate_key_error(self):
        raw = "spark.executor.memory  8g\nspark.executor.memory  16g\n"
        parsed, findings = _parse_properties(raw)
        assert parsed["spark.executor.memory"] == "16g"  # last-wins
        assert len(findings) == 1
        assert findings[0].severity == Severity.ERROR
        assert findings[0].rule == "duplicate-key"

    def test_duplicate_key_same_value_warn(self):
        raw = "spark.executor.memory  8g\nspark.executor.memory  8g\n"
        parsed, findings = _parse_properties(raw)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARN

    def test_equals_separator(self):
        raw = "spark.executor.memory=8g\n"
        parsed, _ = _parse_properties(raw)
        assert parsed["spark.executor.memory"] == "8g"


# ---------------------------------------------------------------------------
# Flatten dict (used by YAML/HOCON)
# ---------------------------------------------------------------------------

class TestFlattenDict:
    def test_flat_passthrough(self):
        d = {"spark.executor.memory": "8g"}
        assert _flatten_dict(d) == {"spark.executor.memory": "8g"}

    def test_nested(self):
        d = {"spark": {"executor": {"memory": "8g"}, "driver": {"memory": "4g"}}}
        flat = _flatten_dict(d)
        assert flat["spark.executor.memory"] == "8g"
        assert flat["spark.driver.memory"] == "4g"

    def test_mixed(self):
        d = {"spark.foo": "bar", "nested": {"key": "val"}}
        flat = _flatten_dict(d)
        assert flat["spark.foo"] == "bar"
        assert flat["nested.key"] == "val"


# ---------------------------------------------------------------------------
# Unified parse_config
# ---------------------------------------------------------------------------

class TestParseConfig:
    def test_json_format(self):
        raw = json.dumps({"conf": {"spark.executor.memory": "8g"}})
        parsed, findings = parse_config(raw, filename="config.json")
        assert "conf" in parsed
        assert parsed["conf"]["spark.executor.memory"] == "8g"

    def test_properties_format(self):
        raw = "spark.executor.memory  8g\nspark.sql.adaptive.enabled  true\n"
        parsed, findings = parse_config(raw, filename="spark-defaults.conf")
        assert parsed["spark.executor.memory"] == "8g"
        assert parsed["spark.sql.adaptive.enabled"] == "true"

    def test_json_duplicate_key_detected(self):
        # JSON with duplicate key
        raw = '{"conf": {"spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.enabled": "false"}}'
        parsed, findings = parse_config(raw, filename="config.json")
        assert any(f.rule == "duplicate-key" for f in findings)


# ---------------------------------------------------------------------------
# End-to-end: lint a properties file
# ---------------------------------------------------------------------------

class TestLintProperties:
    def test_lint_properties_file(self):
        """Lint a spark-defaults.conf and find real issues."""
        raw = (
            "spark.executor.memory  16g\n"
            "spark.executor.instances  200\n"
            "spark.dynamicAllocation.enabled  false\n"
            "spark.sql.adaptive.enabled  false\n"
            "spark.sql.adaptive.coalescePartitions.enabled  true\n"
            "spark.sql.adaptive.skewJoin.enabled  true\n"
            "spark.sql.shuffle.partitions  2000\n"
        )
        report = lint(raw, config_path="spark-defaults.conf")
        rules_found = {f.rule for f in report.findings}
        # Should find AQE contradictions (AQE off but sub-settings on)
        assert "aqe-contradictions" in rules_found

    def test_lint_properties_dup_key(self):
        """Properties files with duplicate keys get duplicate-key findings."""
        raw = (
            "spark.executor.memory  8g\n"
            "spark.executor.memory  16g\n"
        )
        report = lint(raw, config_path="config.properties")
        assert any(f.rule == "duplicate-key" for f in report.findings)

    def test_lint_clean_properties(self):
        """A clean properties file should lint without errors."""
        raw = (
            "spark.executor.memory  8g\n"
            "spark.sql.adaptive.enabled  true\n"
            "spark.serializer  org.apache.spark.serializer.KryoSerializer\n"
        )
        report = lint(raw, config_path="spark-defaults.conf")
        errors = [f for f in report.findings if f.severity == Severity.ERROR]
        assert len(errors) == 0


# ---------------------------------------------------------------------------
# End-to-end: diff across formats
# ---------------------------------------------------------------------------

class TestCrossFormatDiff:
    def test_diff_json_vs_properties(self):
        """Can diff a JSON config against a properties file."""
        from sparkguard.diff import diff_configs

        json_raw = json.dumps({"conf": {"spark.executor.memory": "8g", "spark.driver.memory": "4g"}})
        props_raw = "spark.executor.memory  16g\nspark.driver.memory  4g\n"

        report = diff_configs(json_raw, props_raw, "base.json", "target.conf")
        # Should detect the executor memory change
        changed_keys = [c.key for c in report.changes]
        assert "spark.executor.memory" in changed_keys


# ---------------------------------------------------------------------------
# Script extraction — embedded JSON in .ps1, .py, .sh
# ---------------------------------------------------------------------------

class TestScriptExtraction:
    def test_detect_ps1(self):
        assert detect_format("anything", "submit.ps1") == "script"

    def test_detect_py(self):
        assert detect_format("anything", "submit.py") == "script"

    def test_detect_sh(self):
        assert detect_format("anything", "submit.sh") == "script"

    def test_powershell_here_string(self):
        """Extract Livy JSON from PowerShell @'...'@ here-string."""
        script = '''
$aadToken = $env:AAD_TOKEN
$LivyRequest = @'
{
  "file": "hdfs:///app.jar",
  "className": "com.example.Job",
  "conf": {
    "spark.executor.memory": "8g",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
  }
}
'@
Invoke-RestMethod -Uri "https://livy/batches" -Body $LivyRequest
'''
        parsed, findings = parse_config(script, filename="submit.ps1")
        from sparkguard.utils import get_spark_conf
        conf = get_spark_conf(parsed)
        assert conf["spark.executor.memory"] == "8g"
        assert conf["spark.sql.adaptive.enabled"] == "false"

    def test_python_embedded_json(self):
        """Extract Spark config from a Python script with JSON in a string."""
        script = '''
import requests

payload = """
{
    "sparkConf": {
        "spark.executor.memory": "16g",
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": 200
    }
}
"""

requests.post("https://livy/batches", data=payload)
'''
        parsed, findings = parse_config(script, filename="submit.py")
        from sparkguard.utils import get_spark_conf
        conf = get_spark_conf(parsed)
        assert conf["spark.executor.memory"] == "16g"

    def test_shell_embedded_json(self):
        """Extract Spark config from a bash script with JSON in a heredoc."""
        script = '''#!/bin/bash
curl -X POST https://livy/batches -d '{
  "conf": {
    "spark.executor.memory": "8g",
    "spark.sql.shuffle.partitions": "2000"
  }
}'
'''
        parsed, findings = parse_config(script, filename="submit.sh")
        from sparkguard.utils import get_spark_conf
        conf = get_spark_conf(parsed)
        assert conf["spark.executor.memory"] == "8g"

    def test_no_spark_config_raises(self):
        """Script with no Spark config raises ValueError."""
        script = '''
$x = 42
Write-Output "Hello, World!"
'''
        with pytest.raises(ValueError, match="No embedded Spark config"):
            parse_config(script, filename="noop.ps1")

    def test_lint_ps1_finds_issues(self):
        """End-to-end: lint a .ps1 file and find real Spark issues."""
        script = '''
$body = @'
{
  "conf": {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.shuffle.partitions": "2000"
  }
}
'@
'''
        report = lint(script, config_path="submit.ps1")
        rules = {f.rule for f in report.findings}
        assert "aqe-contradictions" in rules
        assert "high-shuffle-no-aqe" in rules

    def test_multiple_json_blobs_picks_spark_one(self):
        """When a script has multiple JSON objects, pick the one with Spark config."""
        script = '''
$headers = @'
{"Authorization": "Bearer token", "Content-Type": "application/json"}
'@

$body = @'
{
  "conf": {
    "spark.executor.memory": "8g",
    "spark.sql.adaptive.enabled": "true"
  }
}
'@
'''
        parsed, findings = parse_config(script, filename="multi.ps1")
        from sparkguard.utils import get_spark_conf
        conf = get_spark_conf(parsed)
        assert "spark.executor.memory" in conf

"""Tests for SparkGuard verify module (dry-run + history server)."""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest

from sparkguard.verify import (
    DryRunResult,
    JobMetrics,
    MetricsComparison,
    dry_run,
    find_spark_submit,
    _fmt_bytes,
)


# ---------------------------------------------------------------------------
# DryRunResult tests
# ---------------------------------------------------------------------------

class TestDryRunResult:
    def test_accepted_format(self):
        r = DryRunResult(
            accepted=True, spark_warnings=[], spark_errors=[],
            deprecated_keys=[], unrecognized_keys=[], spark_version="3.5.1",
        )
        assert "✓" in r.format_text()
        assert "3.5.1" in r.format_text()

    def test_rejected_format(self):
        r = DryRunResult(
            accepted=False, spark_warnings=[],
            spark_errors=["Invalid value for spark.executor.memory"],
            deprecated_keys=["spark.shuffle.consolidateFiles"],
            unrecognized_keys=[],
        )
        text = r.format_text()
        assert "✗" in text
        assert "deprecated" in text.lower()

    def test_as_dict(self):
        r = DryRunResult(
            accepted=True, spark_warnings=["warn1"], spark_errors=[],
            deprecated_keys=[], unrecognized_keys=[], spark_version="3.5.1",
        )
        d = r.as_dict()
        assert d["accepted"] is True
        assert d["spark_version"] == "3.5.1"
        assert d["spark_warnings"] == ["warn1"]


# ---------------------------------------------------------------------------
# JobMetrics tests
# ---------------------------------------------------------------------------

class TestJobMetrics:
    def test_duration_str_seconds(self):
        m = JobMetrics(app_id="app-1", duration_ms=5000)
        assert m.duration_str == "5.0s"

    def test_duration_str_minutes(self):
        m = JobMetrics(app_id="app-1", duration_ms=180_000)
        assert m.duration_str == "3.0m"

    def test_duration_str_hours(self):
        m = JobMetrics(app_id="app-1", duration_ms=7_200_000)
        assert m.duration_str == "2.0h"

    def test_shuffle_ratio(self):
        m = JobMetrics(
            app_id="app-1",
            total_input_bytes=1_000_000,
            total_shuffle_write_bytes=3_000_000,
        )
        assert m.shuffle_ratio == "3.0x"

    def test_shuffle_ratio_no_input(self):
        m = JobMetrics(app_id="app-1", total_input_bytes=0)
        assert m.shuffle_ratio == "N/A"

    def test_as_dict_complete(self):
        m = JobMetrics(app_id="app-1", app_name="test", duration_ms=5000)
        d = m.as_dict()
        assert d["app_id"] == "app-1"
        assert d["duration"] == "5.0s"
        assert "failed_tasks" in d


# ---------------------------------------------------------------------------
# MetricsComparison tests
# ---------------------------------------------------------------------------

class TestMetricsComparison:
    def test_duration_improvement(self):
        before = JobMetrics(app_id="app-1", duration_ms=180_000)
        after = JobMetrics(app_id="app-2", duration_ms=60_000)
        comp = MetricsComparison(before=before, after=after)
        assert comp.duration_change_pct == pytest.approx(-66.7, abs=0.1)

    def test_duration_regression(self):
        before = JobMetrics(app_id="app-1", duration_ms=60_000)
        after = JobMetrics(app_id="app-2", duration_ms=180_000)
        comp = MetricsComparison(before=before, after=after)
        assert comp.duration_change_pct == pytest.approx(200.0)

    def test_format_text(self):
        before = JobMetrics(app_id="app-1", duration_ms=180_000, failed_tasks=5, killed_tasks=2)
        after = JobMetrics(app_id="app-2", duration_ms=60_000, failed_tasks=0, killed_tasks=0)
        comp = MetricsComparison(before=before, after=after)
        text = comp.format_text()
        assert "Duration:" in text
        assert "✓" in text  # improvement
        assert "Failed:" in text

    def test_as_dict(self):
        before = JobMetrics(app_id="app-1", duration_ms=100_000)
        after = JobMetrics(app_id="app-2", duration_ms=80_000)
        comp = MetricsComparison(before=before, after=after)
        d = comp.as_dict()
        assert "before" in d
        assert "after" in d
        assert d["duration_change_pct"] == -20.0


# ---------------------------------------------------------------------------
# Dry-run with mocked subprocess
# ---------------------------------------------------------------------------

class TestDryRun:
    GOOD_CONFIG = json.dumps({
        "sparkConf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.executor.memory": "8g",
        }
    })

    @patch("sparkguard.verify.find_spark_submit", return_value=None)
    def test_no_spark_submit(self, _mock):
        result = dry_run(self.GOOD_CONFIG, spark_submit_path=None)
        # When no path given AND find returns None, should get error
        result2 = dry_run(self.GOOD_CONFIG)
        assert not result2.accepted
        assert "not found" in result2.spark_errors[0].lower()

    @patch("subprocess.run")
    def test_config_accepted(self, mock_run):
        mock_run.return_value = MagicMock(
            stderr="Running Spark version 3.5.1\nClassNotFoundException: nonexistent",
            stdout="",
            returncode=1,  # expected — class doesn't exist
        )
        result = dry_run(self.GOOD_CONFIG, spark_submit_path="/fake/spark-submit")
        assert result.accepted
        assert result.spark_version == "3.5.1"

    @patch("subprocess.run")
    def test_config_with_deprecated_key(self, mock_run):
        mock_run.return_value = MagicMock(
            stderr=(
                "Running Spark version 3.5.1\n"
                "WARN: spark.shuffle.consolidateFiles is deprecated\n"
                "ClassNotFoundException: nonexistent"
            ),
            stdout="",
            returncode=1,
        )
        result = dry_run(self.GOOD_CONFIG, spark_submit_path="/fake/spark-submit")
        assert result.accepted  # deprecated is a warning, not an error
        assert len(result.deprecated_keys) == 1

    @patch("subprocess.run")
    def test_config_rejected(self, mock_run):
        mock_run.return_value = MagicMock(
            stderr=(
                "Running Spark version 3.5.1\n"
                "IllegalArgumentException: Invalid value for spark.executor.memory\n"
            ),
            stdout="",
            returncode=1,
        )
        result = dry_run(self.GOOD_CONFIG, spark_submit_path="/fake/spark-submit")
        assert not result.accepted
        assert len(result.spark_errors) == 1


# ---------------------------------------------------------------------------
# Utility tests
# ---------------------------------------------------------------------------

class TestFmtBytes:
    def test_bytes(self):
        assert _fmt_bytes(500) == "500.0B"

    def test_megabytes(self):
        assert _fmt_bytes(5 * 1024 * 1024) == "5.0MB"

    def test_gigabytes(self):
        assert _fmt_bytes(2 * 1024 ** 3) == "2.0GB"

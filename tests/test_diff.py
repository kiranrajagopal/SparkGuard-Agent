"""Tests for sparkguard diff."""

import json
import pytest

from sparkguard.diff import diff_configs, format_diff_text
from sparkguard.models import Severity


def _conf(**keys) -> str:
    return json.dumps({"conf": keys})


class TestDiffConfigs:
    def test_no_changes(self):
        raw = _conf(**{"spark.executor.memory": "4g"})
        report = diff_configs(raw, raw)
        assert len(report.changes) == 0
        assert len(report.new_findings) == 0
        assert len(report.resolved_findings) == 0

    def test_detects_value_change(self):
        base = _conf(**{"spark.executor.memory": "4g"})
        target = _conf(**{"spark.executor.memory": "8g"})
        report = diff_configs(base, target)
        assert len(report.changes) == 1
        assert report.changes[0].kind == "changed"
        assert report.changes[0].old_value == "4g"
        assert report.changes[0].new_value == "8g"

    def test_detects_added_key(self):
        base = _conf(**{"spark.executor.memory": "4g"})
        target = _conf(**{
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
        })
        report = diff_configs(base, target)
        added = [c for c in report.changes if c.kind == "added"]
        assert len(added) == 1
        assert added[0].key == "spark.executor.cores"

    def test_detects_removed_key(self):
        base = _conf(**{
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
        })
        target = _conf(**{"spark.executor.memory": "4g"})
        report = diff_configs(base, target)
        removed = [c for c in report.changes if c.kind == "removed"]
        assert len(removed) == 1
        assert removed[0].key == "spark.executor.cores"

    def test_detects_new_regression(self):
        """Adding a bad setting should show up as a new finding."""
        base = _conf(**{
            "spark.sql.adaptive.enabled": "true",
            "spark.executor.memory": "4g",
        })
        target = _conf(**{
            "spark.sql.adaptive.enabled": "false",
            "spark.executor.memory": "4g",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        })
        report = diff_configs(base, target)
        assert report.has_regressions
        aqe = [f for f in report.new_findings if f.rule == "aqe-contradictions"]
        assert len(aqe) == 1

    def test_detects_resolved_issue(self):
        """Fixing a bad config should show the issue as resolved."""
        base = _conf(**{
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        })
        target = _conf(**{
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        })
        report = diff_configs(base, target)
        resolved = [f for f in report.resolved_findings if f.rule == "aqe-contradictions"]
        assert len(resolved) == 1
        assert not report.has_regressions


class TestFormatDiff:
    def test_no_changes_output(self):
        raw = _conf(**{"spark.executor.memory": "4g"})
        report = diff_configs(raw, raw, "a.json", "b.json")
        text = format_diff_text(report)
        assert "No config changes" in text

    def test_regression_output(self):
        base = _conf(**{"spark.sql.adaptive.enabled": "true"})
        target = _conf(**{
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        })
        report = diff_configs(base, target, "old.json", "new.json")
        text = format_diff_text(report)
        assert "NEW issue" in text
        assert "old.json" in text
        assert "new.json" in text

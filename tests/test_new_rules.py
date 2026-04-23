"""Tests for the 5 new adtech-focused rules."""

import json
import pytest

from sparkguard.linter import lint
from sparkguard.models import Severity


def _make_conf(**spark_keys) -> str:
    return json.dumps({"conf": spark_keys})


# ---------------------------------------------------------------------------
# broadcast-threshold-too-high
# ---------------------------------------------------------------------------
class TestBroadcastThreshold:
    def test_threshold_over_500m_is_error(self):
        raw = _make_conf(**{"spark.sql.autoBroadcastJoinThreshold": "1g"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "broadcast-threshold-too-high"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.ERROR

    def test_threshold_300m_is_warn(self):
        raw = _make_conf(**{"spark.sql.autoBroadcastJoinThreshold": "300m"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "broadcast-threshold-too-high"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.WARN

    def test_threshold_100m_is_fine(self):
        raw = _make_conf(**{"spark.sql.autoBroadcastJoinThreshold": "100m"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "broadcast-threshold-too-high"]
        assert len(hits) == 0

    def test_threshold_not_set_is_fine(self):
        raw = _make_conf(**{"spark.executor.memory": "4g"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "broadcast-threshold-too-high"]
        assert len(hits) == 0


# ---------------------------------------------------------------------------
# max-partition-bytes
# ---------------------------------------------------------------------------
class TestMaxPartitionBytes:
    def test_very_large_partition_bytes(self):
        raw = _make_conf(**{"spark.sql.files.maxPartitionBytes": "1g"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "max-partition-bytes"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.WARN

    def test_default_128m_ok(self):
        raw = _make_conf(**{"spark.sql.files.maxPartitionBytes": "128m"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "max-partition-bytes"]
        assert len(hits) == 0


# ---------------------------------------------------------------------------
# heartbeat-timeout-mismatch
# ---------------------------------------------------------------------------
class TestHeartbeatTimeout:
    def test_heartbeat_exceeds_timeout(self):
        raw = _make_conf(**{
            "spark.executor.heartbeatInterval": "120s",
            "spark.network.timeout": "60s",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "heartbeat-timeout-mismatch"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.ERROR

    def test_heartbeat_equals_timeout(self):
        raw = _make_conf(**{
            "spark.executor.heartbeatInterval": "120s",
            "spark.network.timeout": "120s",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "heartbeat-timeout-mismatch"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.ERROR

    def test_heartbeat_over_half(self):
        raw = _make_conf(**{
            "spark.executor.heartbeatInterval": "100s",
            "spark.network.timeout": "180s",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "heartbeat-timeout-mismatch"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.WARN

    def test_heartbeat_healthy_ratio(self):
        raw = _make_conf(**{
            "spark.executor.heartbeatInterval": "30s",
            "spark.network.timeout": "300s",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "heartbeat-timeout-mismatch"]
        assert len(hits) == 0

    def test_mixed_units_ms_and_s(self):
        raw = _make_conf(**{
            "spark.executor.heartbeatInterval": "60000ms",
            "spark.network.timeout": "120s",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "heartbeat-timeout-mismatch"]
        # 60s is exactly half of 120s → should be fine (not > 50%)
        assert len(hits) == 0


# ---------------------------------------------------------------------------
# offheap-missing-size
# ---------------------------------------------------------------------------
class TestOffheap:
    def test_offheap_enabled_no_size(self):
        raw = _make_conf(**{"spark.memory.offHeap.enabled": "true"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "offheap-missing-size"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.ERROR

    def test_offheap_enabled_zero_size(self):
        raw = _make_conf(**{
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "0",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "offheap-missing-size"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.ERROR

    def test_offheap_enabled_with_valid_size(self):
        raw = _make_conf(**{
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "2g",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "offheap-missing-size"]
        assert len(hits) == 0

    def test_offheap_disabled_no_issue(self):
        raw = _make_conf(**{"spark.memory.offHeap.enabled": "false"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "offheap-missing-size"]
        assert len(hits) == 0


# ---------------------------------------------------------------------------
# shuffle-partitions-type
# ---------------------------------------------------------------------------
class TestShufflePartitionsType:
    def test_float_value(self):
        raw = _make_conf(**{"spark.sql.shuffle.partitions": "200.0"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "shuffle-partitions-type"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.WARN

    def test_non_numeric_value(self):
        raw = _make_conf(**{"spark.sql.shuffle.partitions": "auto"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "shuffle-partitions-type"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.ERROR

    def test_clean_integer_ok(self):
        raw = _make_conf(**{"spark.sql.shuffle.partitions": "200"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "shuffle-partitions-type"]
        assert len(hits) == 0

    def test_actual_int_type_ok(self):
        """When the JSON parser produces an int (not a string)."""
        raw = '{"conf": {"spark.sql.shuffle.partitions": 200}}'
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "shuffle-partitions-type"]
        assert len(hits) == 0

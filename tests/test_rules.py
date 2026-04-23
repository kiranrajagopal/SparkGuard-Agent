"""Tests for individual lint rules."""

import json
import pytest

from sparkguard.linter import lint
from sparkguard.models import Severity


def _make_conf(**spark_keys) -> str:
    """Helper: build a minimal Spark submit JSON with given keys."""
    return json.dumps({"conf": spark_keys})


class TestAQEContradictions:
    def test_aqe_off_with_sub_settings(self):
        raw = _make_conf(**{
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
        })
        report = lint(raw)
        aqe_findings = [f for f in report.findings if f.rule == "aqe-contradictions"]
        assert len(aqe_findings) == 1
        assert aqe_findings[0].severity == Severity.ERROR

    def test_aqe_off_without_sub_settings(self):
        raw = _make_conf(**{
            "spark.sql.adaptive.enabled": "false",
        })
        report = lint(raw)
        aqe_findings = [f for f in report.findings if f.rule == "aqe-contradictions"]
        assert len(aqe_findings) == 0

    def test_aqe_on_with_sub_settings_ok(self):
        raw = _make_conf(**{
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        })
        report = lint(raw)
        aqe_findings = [f for f in report.findings if f.rule == "aqe-contradictions"]
        assert len(aqe_findings) == 0


class TestHighShuffleNoAQE:
    def test_high_partitions_aqe_off(self):
        raw = _make_conf(**{
            "spark.sql.shuffle.partitions": "5000",
            "spark.sql.adaptive.enabled": "false",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "high-shuffle-no-aqe"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.WARN

    def test_high_partitions_aqe_on_no_warning(self):
        raw = _make_conf(**{
            "spark.sql.shuffle.partitions": "5000",
            "spark.sql.adaptive.enabled": "true",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "high-shuffle-no-aqe"]
        assert len(hits) == 0


class TestStorageFraction:
    def test_high_storage_fraction(self):
        raw = _make_conf(**{
            "spark.memory.storageFraction": "0.8",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "storage-fraction-no-cache"]
        assert len(hits) == 1

    def test_normal_storage_fraction(self):
        raw = _make_conf(**{
            "spark.memory.storageFraction": "0.5",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "storage-fraction-no-cache"]
        assert len(hits) == 0


class TestDynamicAllocation:
    def test_dynamic_off_high_executors(self):
        raw = _make_conf(**{
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "200",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "dynamic-alloc-off-high-executors"]
        assert len(hits) == 1

    def test_dynamic_off_low_executors_ok(self):
        raw = _make_conf(**{
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "10",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "dynamic-alloc-off-high-executors"]
        assert len(hits) == 0


class TestMemoryOverhead:
    def test_low_overhead(self):
        raw = _make_conf(**{
            "spark.executor.memory": "16g",
            "spark.executor.memoryOverhead": "256m",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "low-memory-overhead"]
        assert len(hits) == 1
        # 10% of 16g = 1638m; 256m is way too low
        assert "256" in hits[0].message

    def test_adequate_overhead(self):
        raw = _make_conf(**{
            "spark.executor.memory": "4g",
            "spark.executor.memoryOverhead": "512m",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "low-memory-overhead"]
        assert len(hits) == 0


class TestContainerMemory:
    def test_oversized_container(self):
        raw = _make_conf(**{
            "spark.executor.memory": "60g",
            "spark.executor.memoryOverhead": "8g",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "container-memory-sanity"]
        assert len(hits) == 1

    def test_reasonable_container(self):
        raw = _make_conf(**{
            "spark.executor.memory": "8g",
            "spark.executor.memoryOverhead": "1g",
        })
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "container-memory-sanity"]
        assert len(hits) == 0


class TestSpeculation:
    def test_speculation_on(self):
        raw = _make_conf(**{"spark.speculation": "true"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "speculation-risk"]
        assert len(hits) == 1

    def test_speculation_off(self):
        raw = _make_conf(**{"spark.speculation": "false"})
        report = lint(raw)
        hits = [f for f in report.findings if f.rule == "speculation-risk"]
        assert len(hits) == 0

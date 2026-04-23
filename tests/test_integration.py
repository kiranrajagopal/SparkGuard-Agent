"""Integration test: the exact scenario that burned 3 hours."""

from sparkguard.linter import lint
from sparkguard.models import Severity


XANDR_CONFIG_WITH_BUG = """{
    "name": "xandr-bidlog-aggregation",
    "conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "2g",
        "spark.executor.instances": "50",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.enabled": "false"
    }
}"""


def test_catches_the_3_hour_bug():
    """This is THE test — the duplicate key that silently disabled AQE."""
    report = lint(XANDR_CONFIG_WITH_BUG, config_path="xandr-bidlog.json")

    # Must find the duplicate key
    dup_findings = [f for f in report.findings if f.rule == "duplicate-key"]
    assert len(dup_findings) == 1
    assert dup_findings[0].severity == Severity.ERROR
    assert "spark.sql.adaptive.enabled" in dup_findings[0].keys

    # Must also catch AQE contradictions (AQE off but sub-settings present)
    aqe_findings = [f for f in report.findings if f.rule == "aqe-contradictions"]
    assert len(aqe_findings) == 1
    assert aqe_findings[0].severity == Severity.ERROR

    # Exit code should be 1 (errors present)
    assert report.error_count >= 2


CLEAN_CONFIG = """{
    "conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.executor.memory": "8g",
        "spark.executor.memoryOverhead": "1g",
        "spark.executor.instances": "20",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
}"""


def test_clean_config_no_errors():
    report = lint(CLEAN_CONFIG)
    errors = [f for f in report.findings if f.severity == Severity.ERROR]
    assert len(errors) == 0

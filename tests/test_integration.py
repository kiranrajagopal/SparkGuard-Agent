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


# ---------------------------------------------------------------------------
# Nested config shapes — real-world platforms
# ---------------------------------------------------------------------------

import json


def test_kubernetes_spark_operator():
    """K8s SparkApplication CRD nests config under spec.sparkConf."""
    config = json.dumps({
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "spec": {
            "type": "Scala",
            "sparkConf": {
                "spark.executor.memory": "16g",
                "spark.sql.adaptive.enabled": "false",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            }
        }
    })
    report = lint(config)
    rules = {f.rule for f in report.findings}
    assert "aqe-contradictions" in rules


def test_airflow_deeply_nested():
    """Airflow SparkSubmitOperator buries config 3-4 levels deep."""
    config = json.dumps({
        "dag": {
            "task_id": "spark_etl",
            "spark_submit_operator": {
                "conf": {
                    "spark.executor.memory": "16g",
                    "spark.executor.instances": 200,
                    "spark.dynamicAllocation.enabled": "false",
                }
            }
        }
    })
    report = lint(config)
    rules = {f.rule for f in report.findings}
    assert "dynamic-alloc-off-high-executors" in rules


def test_nested_yaml_style():
    """YAML/HOCON nested configs: spark.executor.memory split into spark: executor: memory."""
    config = json.dumps({
        "spark": {
            "executor": {"memory": "16g", "instances": 200},
            "dynamicAllocation": {"enabled": "false"},
            "sql": {
                "adaptive": {
                    "enabled": "false",
                    "coalescePartitions": {"enabled": "true"},
                }
            }
        }
    })
    report = lint(config)
    rules = {f.rule for f in report.findings}
    assert "aqe-contradictions" in rules
    assert "dynamic-alloc-off-high-executors" in rules


def test_emr_key_value_list():
    """EMR/Dataproc configs use [{Key: ..., Value: ...}] lists."""
    config = json.dumps({
        "Steps": [
            {"Key": "spark.executor.memory", "Value": "16g"},
            {"Key": "spark.executor.instances", "Value": "200"},
            {"Key": "spark.dynamicAllocation.enabled", "Value": "false"},
            {"Key": "spark.sql.adaptive.enabled", "Value": "false"},
            {"Key": "spark.sql.adaptive.coalescePartitions.enabled", "Value": "true"},
        ]
    })
    report = lint(config)
    rules = {f.rule for f in report.findings}
    assert "aqe-contradictions" in rules


def test_databricks_job_config():
    """Databricks job API nests under new_cluster.spark_conf."""
    config = json.dumps({
        "name": "my-job",
        "new_cluster": {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "spark_conf": {
                "spark.executor.memory": "16g",
                "spark.memory.offHeap.enabled": "true",
            }
        }
    })
    report = lint(config)
    rules = {f.rule for f in report.findings}
    # Off-heap enabled without size → error
    assert "offheap-missing-size" in rules

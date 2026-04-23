"""Tests for the config generator."""

import json
import pytest

from sparkguard.generator import (
    WorkloadProfile,
    collect_profile_from_answers,
    generate_config,
    format_config_json,
)
from sparkguard.linter import lint
from sparkguard.models import Severity


class TestGenerateConfig:
    """Test that generated configs are sane and pass our own linter."""

    def _make_profile(self, **overrides) -> WorkloadProfile:
        defaults = {
            "dataset_size_gb": 50,
            "job_type": "etl",
            "has_joins": True,
            "has_skew": False,
            "uses_caching": False,
            "uses_pyspark": True,
            "uses_udfs": False,
            "cluster_node_memory_gb": 64,
            "cluster_node_cores": 16,
            "cluster_node_count": 10,
            "output_format": "parquet",
        }
        defaults.update(overrides)
        return WorkloadProfile(**defaults)

    def test_basic_etl_config(self):
        profile = self._make_profile()
        conf = generate_config(profile)

        assert conf["spark.sql.adaptive.enabled"] == "true"
        assert conf["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
        assert "spark.executor.memory" in conf
        assert "spark.executor.cores" in conf
        assert conf["spark.dynamicAllocation.enabled"] == "true"

    def test_generated_config_passes_linter(self):
        """The generator should never produce a config that fails its own linter with errors."""
        profile = self._make_profile()
        conf = generate_config(profile)
        raw_json = format_config_json(conf)
        report = lint(raw_json)
        errors = [f for f in report.findings if f.severity == Severity.ERROR]
        assert errors == [], f"Generated config has errors: {errors}"

    def test_pyspark_udf_gets_extra_overhead(self):
        profile = self._make_profile(uses_pyspark=True, uses_udfs=True)
        conf = generate_config(profile)
        assert "spark.executor.pyspark.memory" in conf
        # Overhead should be 20% for UDF users
        overhead_mb = int(conf["spark.executor.memoryOverhead"].rstrip("m"))
        exec_mem_gb = int(conf["spark.executor.memory"].rstrip("g"))
        assert overhead_mb >= exec_mem_gb * 1024 * 0.18  # ~20%, allow slight rounding

    def test_streaming_disables_dynamic_allocation(self):
        profile = self._make_profile(job_type="streaming")
        conf = generate_config(profile)
        assert conf["spark.dynamicAllocation.enabled"] == "false"
        assert "spark.executor.instances" in conf

    def test_delta_output_adds_extensions(self):
        profile = self._make_profile(output_format="delta")
        conf = generate_config(profile)
        assert "spark.sql.extensions" in conf
        assert "delta" in conf["spark.sql.extensions"].lower()

    def test_skew_with_joins_enables_skew_join(self):
        profile = self._make_profile(has_skew=True, has_joins=True)
        conf = generate_config(profile)
        assert conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"

    def test_caching_increases_storage_fraction(self):
        no_cache = self._make_profile(uses_caching=False)
        with_cache = self._make_profile(uses_caching=True)
        conf_no = generate_config(no_cache)
        conf_yes = generate_config(with_cache)
        assert float(conf_yes["spark.memory.storageFraction"]) > float(
            conf_no["spark.memory.storageFraction"]
        )

    def test_small_dataset_fewer_partitions(self):
        small = self._make_profile(dataset_size_gb=0.5)
        large = self._make_profile(dataset_size_gb=750)
        conf_small = generate_config(small)
        conf_large = generate_config(large)
        assert int(conf_small["spark.sql.shuffle.partitions"]) < int(
            conf_large["spark.sql.shuffle.partitions"]
        )

    def test_ml_job_gets_kryo_buffer(self):
        profile = self._make_profile(job_type="ml")
        conf = generate_config(profile)
        assert "spark.kryoserializer.buffer.max" in conf


class TestCollectProfileFromAnswers:
    """Test the non-interactive answer dict path."""

    def test_basic_answers(self):
        answers = {
            "dataset_size_gb": "10-100 GB",
            "job_type": "ETL / transform",
            "has_joins": "Yes",
            "has_skew": "No",
            "uses_caching": "No",
            "uses_pyspark": "PySpark",
            "uses_udfs": "No",
            "cluster_node_memory_gb": "64 GB",
            "cluster_node_cores": "16",
            "cluster_node_count": "10-20",
            "output_format": "Parquet",
        }
        profile = collect_profile_from_answers(answers)
        assert profile.dataset_size_gb == 50
        assert profile.job_type == "etl"
        assert profile.has_joins is True
        assert profile.uses_pyspark is True
        assert profile.cluster_node_count == 15

    def test_generated_from_answers_passes_linter(self):
        """End-to-end: answers → profile → config → lint with zero errors."""
        answers = {
            "dataset_size_gb": "100-500 GB",
            "job_type": "Aggregation / reporting",
            "has_joins": "Yes",
            "has_skew": "Yes",
            "uses_caching": "Yes",
            "uses_pyspark": "PySpark",
            "uses_udfs": "Yes",
            "cluster_node_memory_gb": "128 GB",
            "cluster_node_cores": "32",
            "cluster_node_count": "20-50",
            "output_format": "Delta",
        }
        profile = collect_profile_from_answers(answers)
        conf = generate_config(profile)
        raw_json = format_config_json(conf)
        report = lint(raw_json)
        errors = [f for f in report.findings if f.severity == Severity.ERROR]
        assert errors == [], f"Generated config has errors: {[e.message for e in errors]}"

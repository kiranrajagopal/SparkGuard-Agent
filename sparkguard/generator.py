"""Interactive config generator — asks about your workload, produces a tuned Spark config."""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass, field
from typing import Any


@dataclass
class WorkloadProfile:
    """Answers collected from the user."""

    dataset_size_gb: float = 0
    job_type: str = ""           # etl, aggregation, ml, streaming, sql_analytics
    has_joins: bool = False
    has_skew: bool = False
    uses_caching: bool = False
    uses_pyspark: bool = False
    uses_udfs: bool = False
    cluster_node_memory_gb: int = 64
    cluster_node_cores: int = 16
    cluster_node_count: int = 10
    output_format: str = ""      # parquet, delta, orc, csv, json
    write_mode: str = ""         # overwrite, append
    spark_version: str = "3.5"


# ---------------------------------------------------------------------------
# Questions
# ---------------------------------------------------------------------------

@dataclass
class Question:
    key: str
    prompt: str
    choices: list[str] | None = None
    default: str = ""
    parser: Any = None           # callable(str) -> value


QUESTIONS: list[Question] = [
    Question(
        key="dataset_size_gb",
        prompt="What is the approximate input dataset size?",
        choices=["< 1 GB", "1-10 GB", "10-100 GB", "100-500 GB", "500 GB - 1 TB", "> 1 TB"],
        parser=lambda choice: {
            "< 1 GB": 0.5, "1-10 GB": 5, "10-100 GB": 50,
            "100-500 GB": 300, "500 GB - 1 TB": 750, "> 1 TB": 1500,
        }.get(choice, 50),
    ),
    Question(
        key="job_type",
        prompt="What type of Spark job is this?",
        choices=["ETL / transform", "Aggregation / reporting", "ML training", "Streaming", "SQL analytics"],
        parser=lambda choice: {
            "ETL / transform": "etl", "Aggregation / reporting": "aggregation",
            "ML training": "ml", "Streaming": "streaming", "SQL analytics": "sql_analytics",
        }.get(choice, "etl"),
    ),
    Question(
        key="has_joins",
        prompt="Does your job perform joins (especially large table joins)?",
        choices=["Yes", "No"],
        default="Yes",
        parser=lambda c: c.lower().startswith("y"),
    ),
    Question(
        key="has_skew",
        prompt="Do you have data skew (some keys with far more rows than others)?",
        choices=["Yes", "No", "Not sure"],
        default="Not sure",
        parser=lambda c: c.lower().startswith("y") or c.lower() == "not sure",
    ),
    Question(
        key="uses_caching",
        prompt="Does your job cache/persist DataFrames or RDDs?",
        choices=["Yes", "No"],
        default="No",
        parser=lambda c: c.lower().startswith("y"),
    ),
    Question(
        key="uses_pyspark",
        prompt="Are you using PySpark (Python) or Scala?",
        choices=["PySpark", "Scala"],
        default="PySpark",
        parser=lambda c: c.lower().startswith("py"),
    ),
    Question(
        key="uses_udfs",
        prompt="Do you use Python UDFs or Pandas UDFs?",
        choices=["Yes", "No"],
        default="No",
        parser=lambda c: c.lower().startswith("y"),
    ),
    Question(
        key="cluster_node_memory_gb",
        prompt="How much memory (GB) does each worker node have?",
        choices=["16 GB", "32 GB", "64 GB", "128 GB", "256 GB"],
        default="64 GB",
        parser=lambda c: int("".join(ch for ch in c if ch.isdigit()) or "64"),
    ),
    Question(
        key="cluster_node_cores",
        prompt="How many CPU cores per worker node?",
        choices=["4", "8", "16", "32", "64"],
        default="16",
        parser=lambda c: int("".join(ch for ch in c if ch.isdigit()) or "16"),
    ),
    Question(
        key="cluster_node_count",
        prompt="How many worker nodes in your cluster?",
        choices=["1-5", "5-10", "10-20", "20-50", "50+"],
        default="10-20",
        parser=lambda choice: {
            "1-5": 3, "5-10": 8, "10-20": 15, "20-50": 35, "50+": 60,
        }.get(choice, 15),
    ),
    Question(
        key="output_format",
        prompt="What is the output format?",
        choices=["Parquet", "Delta", "ORC", "CSV/JSON", "Database sink"],
        default="Parquet",
        parser=lambda c: c.lower().split("/")[0].split(" ")[0],
    ),
]


# ---------------------------------------------------------------------------
# Interactive prompt
# ---------------------------------------------------------------------------

def _ask_question(q: Question) -> Any:
    """Ask a single question interactively and return the parsed answer."""
    print(f"\n  {q.prompt}")
    if q.choices:
        for i, choice in enumerate(q.choices, 1):
            default_marker = " (default)" if choice == q.default else ""
            print(f"    {i}. {choice}{default_marker}")
        raw = input("  > ").strip()
        if not raw and q.default:
            raw = q.default
        elif raw.isdigit() and 1 <= int(raw) <= len(q.choices):
            raw = q.choices[int(raw) - 1]
    else:
        if q.default:
            print(f"    (default: {q.default})")
        raw = input("  > ").strip()
        if not raw:
            raw = q.default

    if q.parser:
        return q.parser(raw)
    return raw


def collect_profile_interactive() -> WorkloadProfile:
    """Ask the user all questions and build a WorkloadProfile."""
    print("\n━━━ SparkGuard Config Generator ━━━")
    print("Answer a few questions about your workload and I'll generate")
    print("an optimized Spark config.\n")

    profile = WorkloadProfile()
    for q in QUESTIONS:
        value = _ask_question(q)
        setattr(profile, q.key, value)

    return profile


def collect_profile_from_answers(answers: dict[str, str]) -> WorkloadProfile:
    """Build a WorkloadProfile from a pre-filled answer dict (for non-interactive / testing).

    Keys should match Question.key, values should be the display-text of the chosen option.
    """
    profile = WorkloadProfile()
    for q in QUESTIONS:
        if q.key in answers:
            raw = answers[q.key]
            value = q.parser(raw) if q.parser else raw
            setattr(profile, q.key, value)
    return profile


# ---------------------------------------------------------------------------
# Config generation logic
# ---------------------------------------------------------------------------

def generate_config(profile: WorkloadProfile) -> dict[str, Any]:
    """Generate a recommended Spark config dict from a WorkloadProfile."""
    conf: dict[str, str] = {}

    # -- AQE (always on for 3.2+)
    conf["spark.sql.adaptive.enabled"] = "true"
    conf["spark.sql.adaptive.coalescePartitions.enabled"] = "true"

    if profile.has_skew and profile.has_joins:
        conf["spark.sql.adaptive.skewJoin.enabled"] = "true"
        conf["spark.sql.adaptive.skewJoin.skewedPartitionFactor"] = "5"
        conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"] = "256m"

    # -- Serializer
    conf["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"

    # -- Executor sizing
    # Best practice: 5 cores per executor, leave 1 core for OS/YARN
    usable_cores = profile.cluster_node_cores - 1
    cores_per_exec = min(5, usable_cores)
    executors_per_node = max(1, usable_cores // cores_per_exec)

    # Memory per executor: leave ~1 GB for OS overhead per node
    usable_mem_per_node = profile.cluster_node_memory_gb - 1
    mem_per_exec_gb = max(1, usable_mem_per_node // executors_per_node)
    # Cap at reasonable sizes
    mem_per_exec_gb = min(mem_per_exec_gb, 32)

    conf["spark.executor.cores"] = str(cores_per_exec)
    conf["spark.executor.memory"] = f"{mem_per_exec_gb}g"

    # -- Memory overhead
    overhead_pct = 0.10
    if profile.uses_pyspark:
        overhead_pct = 0.15
    if profile.uses_udfs:
        overhead_pct = 0.20

    overhead_mb = max(384, int(mem_per_exec_gb * 1024 * overhead_pct))
    conf["spark.executor.memoryOverhead"] = f"{overhead_mb}m"

    if profile.uses_pyspark and profile.uses_udfs:
        pyspark_mem = max(512, int(mem_per_exec_gb * 1024 * 0.10))
        conf["spark.executor.pyspark.memory"] = f"{pyspark_mem}m"

    # -- Dynamic allocation
    total_executors = executors_per_node * profile.cluster_node_count
    conf["spark.dynamicAllocation.enabled"] = "true"
    conf["spark.dynamicAllocation.minExecutors"] = str(max(1, total_executors // 10))
    conf["spark.dynamicAllocation.maxExecutors"] = str(total_executors)
    conf["spark.dynamicAllocation.executorIdleTimeout"] = "60s"

    if profile.job_type == "streaming":
        # Streaming needs stable executors
        conf["spark.dynamicAllocation.enabled"] = "false"
        conf["spark.executor.instances"] = str(max(2, total_executors // 2))
        del conf["spark.dynamicAllocation.minExecutors"]
        del conf["spark.dynamicAllocation.maxExecutors"]
        del conf["spark.dynamicAllocation.executorIdleTimeout"]

    # -- Shuffle partitions
    if profile.dataset_size_gb <= 1:
        shuffle_partitions = 20
    elif profile.dataset_size_gb <= 10:
        shuffle_partitions = 200
    elif profile.dataset_size_gb <= 100:
        shuffle_partitions = 500
    elif profile.dataset_size_gb <= 500:
        shuffle_partitions = 1000
    else:
        shuffle_partitions = 2000

    conf["spark.sql.shuffle.partitions"] = str(shuffle_partitions)

    # -- Broadcast threshold
    if profile.has_joins:
        if profile.dataset_size_gb <= 10:
            conf["spark.sql.autoBroadcastJoinThreshold"] = "50m"
        elif profile.dataset_size_gb <= 100:
            conf["spark.sql.autoBroadcastJoinThreshold"] = "30m"
        else:
            conf["spark.sql.autoBroadcastJoinThreshold"] = "10m"

    # -- Caching
    if profile.uses_caching:
        conf["spark.memory.storageFraction"] = "0.6"
    else:
        conf["spark.memory.storageFraction"] = "0.3"

    # -- Output-specific
    if profile.output_format == "delta":
        conf["spark.sql.extensions"] = "io.delta.sql.DeltaSparkSessionExtension"
        conf["spark.sql.catalog.spark_catalog"] = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        conf["spark.databricks.delta.optimizeWrite.enabled"] = "true"

    if profile.output_format == "parquet":
        conf["spark.sql.parquet.compression.codec"] = "zstd"

    # -- ML-specific
    if profile.job_type == "ml":
        conf["spark.kryoserializer.buffer.max"] = "1024m"

    # -- Speculation: safe only with idempotent output
    if profile.output_format in ("delta", "parquet") and profile.job_type != "streaming":
        conf["spark.speculation"] = "false"

    return conf


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def format_config_json(conf: dict[str, str], name: str = "my-spark-job") -> str:
    """Format the config as a ready-to-use Spark submit JSON."""
    output = {"name": name, "conf": conf}
    return json.dumps(output, indent=4)


def format_config_with_explanations(conf: dict[str, str], profile: WorkloadProfile) -> str:
    """Format the config with inline explanations for each setting."""
    lines: list[str] = []
    lines.append("━━━ Generated Spark Config ━━━\n")
    lines.append(f"Workload: {profile.job_type} | ~{profile.dataset_size_gb} GB input")
    lines.append(f"Cluster:  {profile.cluster_node_count} nodes × {profile.cluster_node_cores} cores × {profile.cluster_node_memory_gb} GB")
    lines.append(f"Language: {'PySpark' if profile.uses_pyspark else 'Scala'}")
    lines.append("")

    explanations: dict[str, str] = {
        "spark.sql.adaptive.enabled": "AQE auto-optimizes shuffles, joins, and skew at runtime",
        "spark.sql.adaptive.coalescePartitions.enabled": "Merges small post-shuffle partitions automatically",
        "spark.sql.adaptive.skewJoin.enabled": "Splits skewed partitions during joins for even distribution",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "Partition is skewed if N× larger than median",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "Minimum partition size to consider skewed",
        "spark.serializer": "Kryo is 10× faster than Java serialization",
        "spark.executor.cores": "5 cores/executor is the sweet spot for HDFS throughput",
        "spark.executor.memory": "Sized to fit multiple executors per node with OS headroom",
        "spark.executor.memoryOverhead": f"{'20%' if profile.uses_udfs else '15%' if profile.uses_pyspark else '10%'} of executor memory for off-heap / Python",
        "spark.executor.pyspark.memory": "Dedicated memory for Pandas UDF / Arrow operations",
        "spark.dynamicAllocation.enabled": "Auto-scales executors to match workload",
        "spark.dynamicAllocation.minExecutors": "Floor to avoid cold-start on ramp-up",
        "spark.dynamicAllocation.maxExecutors": "Ceiling based on total cluster capacity",
        "spark.dynamicAllocation.executorIdleTimeout": "Release idle executors after this timeout",
        "spark.executor.instances": "Fixed executor count for streaming stability",
        "spark.sql.shuffle.partitions": f"Scaled for ~{profile.dataset_size_gb} GB dataset (AQE will coalesce further)",
        "spark.sql.autoBroadcastJoinThreshold": "Tables under this size are broadcast to avoid shuffles",
        "spark.memory.storageFraction": f"{'Higher' if profile.uses_caching else 'Lower'} — {'caching in use' if profile.uses_caching else 'prioritize execution memory'}",
        "spark.sql.extensions": "Delta Lake SQL extensions",
        "spark.sql.catalog.spark_catalog": "Delta Lake catalog integration",
        "spark.databricks.delta.optimizeWrite.enabled": "Reduces small files on write",
        "spark.sql.parquet.compression.codec": "Zstd gives best compression/speed ratio",
        "spark.kryoserializer.buffer.max": "Larger buffer for ML model serialization",
        "spark.speculation": "Off — avoids duplicate writes with file-based output",
    }

    for key, value in conf.items():
        explanation = explanations.get(key, "")
        lines.append(f"  {key}: {value}")
        if explanation:
            lines.append(f"    └─ {explanation}")

    lines.append("")
    lines.append("━━━ JSON (copy-paste ready) ━━━")
    lines.append(format_config_json(conf))

    return "\n".join(lines)

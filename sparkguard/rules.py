"""Built-in lint rules for Spark config analysis."""

from __future__ import annotations

from sparkguard.engine import rule
from sparkguard.models import Finding, Severity
from sparkguard.utils import as_bool, get_spark_conf, parse_memory


# ---------------------------------------------------------------------------
# Rule: contradictory AQE settings
# ---------------------------------------------------------------------------
@rule("aqe-contradictions")
def check_aqe_contradictions(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    aqe_enabled = as_bool(conf.get("spark.sql.adaptive.enabled", True))  # default True in Spark 3.2+

    aqe_sub_keys = [
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
        "spark.sql.adaptive.localShuffleReader.enabled",
    ]

    if aqe_enabled is False:
        present_sub = [k for k in aqe_sub_keys if k in conf]
        if present_sub:
            findings.append(Finding(
                severity=Severity.ERROR,
                keys=["spark.sql.adaptive.enabled"] + present_sub,
                message=(
                    "AQE is explicitly disabled but AQE sub-settings are configured. "
                    "These settings have no effect when AQE is off."
                ),
                recommendation=(
                    "Either enable AQE (spark.sql.adaptive.enabled=true) or remove the "
                    "dead AQE sub-settings to avoid confusion."
                ),
                rule="aqe-contradictions",
            ))

    return findings


# ---------------------------------------------------------------------------
# Rule: high shuffle partitions without AQE
# ---------------------------------------------------------------------------
@rule("high-shuffle-no-aqe")
def check_high_shuffle_no_aqe(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    partitions = conf.get("spark.sql.shuffle.partitions")
    aqe_enabled = as_bool(conf.get("spark.sql.adaptive.enabled", True))

    if partitions is not None:
        try:
            num = int(partitions)
        except (ValueError, TypeError):
            return findings
        if num >= 2000 and aqe_enabled is False:
            findings.append(Finding(
                severity=Severity.WARN,
                keys=["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled"],
                message=(
                    f"shuffle.partitions is set to {num} with AQE disabled. "
                    "Without AQE coalescing, this creates excessive small partitions for "
                    "smaller stages and cannot adapt to data skew."
                ),
                recommendation=(
                    "Enable AQE (spark.sql.adaptive.enabled=true) so Spark can auto-coalesce "
                    "partitions, or lower shuffle.partitions to match your data volume."
                ),
                rule="high-shuffle-no-aqe",
            ))

    return findings


# ---------------------------------------------------------------------------
# Rule: high storageFraction without caching
# ---------------------------------------------------------------------------
@rule("storage-fraction-no-cache")
def check_storage_fraction_no_cache(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    frac = conf.get("spark.memory.storageFraction")
    if frac is not None:
        try:
            frac_val = float(frac)
        except (ValueError, TypeError):
            return findings
        if frac_val > 0.6:
            # Check if any caching-related setting is present
            cache_hints = [
                "spark.storage.memoryMapThreshold",
                "spark.rdd.compress",
            ]
            has_cache_hint = any(k in conf for k in cache_hints)
            # Also check storageLevel or persist indicators — but those are code-level (Layer 2)
            findings.append(Finding(
                severity=Severity.WARN,
                keys=["spark.memory.storageFraction"],
                message=(
                    f"storageFraction is set to {frac_val} (>{0.6}). This reserves a large "
                    "portion of memory for cached data, reducing execution memory. "
                    "If your job doesn't heavily cache/persist RDDs or DataFrames, this wastes memory."
                ),
                recommendation=(
                    "Lower spark.memory.storageFraction (default 0.5) unless you are "
                    "intentionally caching large datasets. Layer 2 analysis can verify cache usage."
                ),
                rule="storage-fraction-no-cache",
            ))

    return findings


# ---------------------------------------------------------------------------
# Rule: dynamic allocation off + high executor count
# ---------------------------------------------------------------------------
@rule("dynamic-alloc-off-high-executors")
def check_dynamic_alloc(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    dyn_alloc = as_bool(conf.get("spark.dynamicAllocation.enabled"))
    num_exec = conf.get("spark.executor.instances")

    if dyn_alloc is False and num_exec is not None:
        try:
            n = int(num_exec)
        except (ValueError, TypeError):
            return findings
        if n >= 100:
            findings.append(Finding(
                severity=Severity.WARN,
                keys=["spark.dynamicAllocation.enabled", "spark.executor.instances"],
                message=(
                    f"Dynamic allocation is disabled with {n} fixed executors. "
                    "This wastes cluster resources during idle stages and prevents "
                    "scale-down between jobs."
                ),
                recommendation=(
                    "Enable dynamic allocation (spark.dynamicAllocation.enabled=true) "
                    "so executors scale with workload, or justify the fixed count in a comment."
                ),
                rule="dynamic-alloc-off-high-executors",
            ))

    return findings


# ---------------------------------------------------------------------------
# Rule: memoryOverhead too low relative to executorMemory
# ---------------------------------------------------------------------------
@rule("low-memory-overhead")
def check_memory_overhead(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    exec_mem_str = conf.get("spark.executor.memory")
    overhead_str = conf.get("spark.executor.memoryOverhead") or conf.get(
        "spark.yarn.executor.memoryOverhead"
    )

    if exec_mem_str is None or overhead_str is None:
        return findings

    exec_mem = parse_memory(exec_mem_str)
    overhead = parse_memory(overhead_str)

    if exec_mem is None or overhead is None:
        return findings

    # YARN default overhead = max(384MB, 10% of executorMemory)
    recommended_min = max(384, int(exec_mem * 0.10))

    if overhead < recommended_min:
        ratio_pct = round(overhead / exec_mem * 100, 1) if exec_mem > 0 else 0
        findings.append(Finding(
            severity=Severity.WARN,
            keys=["spark.executor.memoryOverhead", "spark.executor.memory"],
            message=(
                f"memoryOverhead ({overhead}m) is only {ratio_pct}% of executorMemory "
                f"({exec_mem}m). YARN default is max(384m, 10%). Low overhead causes "
                "OOM kills from off-heap / direct-buffer usage."
            ),
            recommendation=(
                f"Set spark.executor.memoryOverhead to at least {recommended_min}m "
                f"(10% of {exec_mem}m). For PySpark or heavy shuffle jobs, use 15-20%."
            ),
            rule="low-memory-overhead",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: resource math — total container memory sanity check
# ---------------------------------------------------------------------------
@rule("container-memory-sanity")
def check_container_memory(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    exec_mem_str = conf.get("spark.executor.memory")
    overhead_str = conf.get("spark.executor.memoryOverhead") or conf.get(
        "spark.yarn.executor.memoryOverhead"
    )

    if exec_mem_str is None:
        return findings

    exec_mem = parse_memory(exec_mem_str)
    if exec_mem is None:
        return findings

    overhead = parse_memory(overhead_str) if overhead_str else max(384, int(exec_mem * 0.10))

    pyspark_mem_str = conf.get("spark.executor.pyspark.memory")
    pyspark_mem = parse_memory(pyspark_mem_str) if pyspark_mem_str else 0

    total_mb = exec_mem + overhead + (pyspark_mem or 0)

    # Flag if total exceeds 64 GB (common YARN node ceiling)
    if total_mb > 65536:
        findings.append(Finding(
            severity=Severity.WARN,
            keys=[
                "spark.executor.memory",
                "spark.executor.memoryOverhead",
            ],
            message=(
                f"Total executor container size is ~{total_mb}m ({total_mb / 1024:.1f}g). "
                "This exceeds 64 GB, which is larger than many YARN node managers. "
                "Containers this large reduce scheduling flexibility."
            ),
            recommendation=(
                "Check your cluster's yarn.nodemanager.resource.memory-mb. "
                "Consider using more executors with less memory each."
            ),
            rule="container-memory-sanity",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: missing recommended Spark 3.5 settings
# ---------------------------------------------------------------------------
@rule("missing-spark35-recommendations")
def check_spark35_recommendations(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    recommended = {
        "spark.sql.adaptive.enabled": (
            "AQE should be explicitly enabled for Spark 3.5 workloads. "
            "It optimises shuffle partitions, join strategies, and skew handling at runtime."
        ),
        "spark.serializer": (
            "KryoSerializer (org.apache.spark.serializer.KryoSerializer) is recommended "
            "over JavaSerializer for performance."
        ),
        "spark.sql.adaptive.skewJoin.enabled": (
            "Skew join optimization should be enabled for large join-heavy workloads."
        ),
    }

    for key, reason in recommended.items():
        if key not in conf:
            findings.append(Finding(
                severity=Severity.INFO,
                keys=[key],
                message=f"Recommended setting '{key}' is not present. {reason}",
                recommendation=f"Consider adding '{key}' to your Spark config.",
                rule="missing-spark35-recommendations",
            ))

    # Serializer check: present but not Kryo
    ser = conf.get("spark.serializer", "")
    if ser and "Kryo" not in str(ser):
        findings.append(Finding(
            severity=Severity.INFO,
            keys=["spark.serializer"],
            message=(
                f"Serializer is set to '{ser}'. KryoSerializer is significantly faster "
                "for most workloads."
            ),
            recommendation=(
                "Set spark.serializer=org.apache.spark.serializer.KryoSerializer "
                "unless you have a specific reason not to."
            ),
            rule="missing-spark35-recommendations",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: AQE enabled but explicitly setting low shuffle partitions
# ---------------------------------------------------------------------------
@rule("aqe-with-low-partitions")
def check_aqe_with_low_partitions(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    aqe_enabled = as_bool(conf.get("spark.sql.adaptive.enabled", True))
    partitions = conf.get("spark.sql.shuffle.partitions")

    if aqe_enabled is True and partitions is not None:
        try:
            num = int(partitions)
        except (ValueError, TypeError):
            return findings
        # If someone sets a very low partition count *with* AQE on, the initial partition
        # count limits what AQE can coalesce from. Not necessarily bad, but worth noting.
        if num <= 10:
            findings.append(Finding(
                severity=Severity.INFO,
                keys=["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled"],
                message=(
                    f"shuffle.partitions={num} with AQE enabled. AQE coalesces from "
                    "the initial partition count downward — a very low starting count "
                    "limits AQE's ability to optimize."
                ),
                recommendation=(
                    "Consider using the default 200 or higher and letting AQE coalesce."
                ),
                rule="aqe-with-low-partitions",
            ))

    return findings


# ---------------------------------------------------------------------------
# Rule: speculation enabled without idempotent writes
# ---------------------------------------------------------------------------
@rule("speculation-risk")
def check_speculation(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    spec = as_bool(conf.get("spark.speculation"))
    if spec is True:
        findings.append(Finding(
            severity=Severity.WARN,
            keys=["spark.speculation"],
            message=(
                "Task speculation is enabled. Speculative tasks can cause duplicate "
                "writes if your output format/sink is not idempotent."
            ),
            recommendation=(
                "Ensure your write path is idempotent (e.g., using overwrite mode, "
                "Delta Lake, or a committer that handles speculation). Otherwise disable speculation."
            ),
            rule="speculation-risk",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: broadcast join threshold too high — kills driver memory
# ---------------------------------------------------------------------------
@rule("broadcast-threshold-too-high")
def check_broadcast_threshold(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    threshold_str = conf.get("spark.sql.autoBroadcastJoinThreshold")
    if threshold_str is None:
        return findings

    threshold = parse_memory(str(threshold_str))
    if threshold is None:
        return findings

    # > 500 MB is dangerous — driver collects the whole table into memory
    if threshold > 512:
        findings.append(Finding(
            severity=Severity.ERROR,
            keys=["spark.sql.autoBroadcastJoinThreshold"],
            message=(
                f"autoBroadcastJoinThreshold is set to {threshold_str} (~{threshold}m). "
                "Values above ~500 MB risk driver OOM because broadcast tables are "
                "collected to the driver then shipped to every executor."
            ),
            recommendation=(
                "Lower autoBroadcastJoinThreshold to 256m or less. For very large "
                "dimension tables, use explicit broadcast() hints on small tables only."
            ),
            rule="broadcast-threshold-too-high",
        ))
    elif threshold > 256:
        findings.append(Finding(
            severity=Severity.WARN,
            keys=["spark.sql.autoBroadcastJoinThreshold"],
            message=(
                f"autoBroadcastJoinThreshold is {threshold_str} (~{threshold}m). "
                "This is on the high side — tables near this limit may cause driver "
                "memory pressure during broadcast collection."
            ),
            recommendation=(
                "Consider lowering to 100-256m unless you've confirmed driver memory "
                "can handle it. Monitor driver heap usage."
            ),
            rule="broadcast-threshold-too-high",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: maxPartitionBytes mismatch with small-file workloads
# ---------------------------------------------------------------------------
@rule("max-partition-bytes")
def check_max_partition_bytes(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    max_bytes_str = conf.get("spark.sql.files.maxPartitionBytes")
    if max_bytes_str is None:
        return findings

    max_bytes = parse_memory(str(max_bytes_str))
    if max_bytes is None:
        return findings

    # Default is 128m. Very large values with many small files = fewer but huge partitions = stragglers
    if max_bytes > 512:
        findings.append(Finding(
            severity=Severity.WARN,
            keys=["spark.sql.files.maxPartitionBytes"],
            message=(
                f"maxPartitionBytes is {max_bytes_str} (~{max_bytes}m). "
                "With many small input files (common in event log / adtech pipelines), "
                "large partition bytes packs too many files into one partition, "
                "creating stragglers while other tasks finish quickly."
            ),
            recommendation=(
                "Keep maxPartitionBytes at 128m (default) or lower for small-file workloads. "
                "Use file compaction upstream if possible."
            ),
            rule="max-partition-bytes",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: heartbeat/network timeout mismatch — healthy executors get killed
# ---------------------------------------------------------------------------
@rule("heartbeat-timeout-mismatch")
def check_heartbeat_timeout(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    heartbeat_str = conf.get("spark.executor.heartbeatInterval")
    timeout_str = conf.get("spark.network.timeout")

    if heartbeat_str is None or timeout_str is None:
        return findings

    def _parse_duration_seconds(val: str) -> int | None:
        val = str(val).strip().lower()
        try:
            if val.endswith("ms"):
                return int(float(val[:-2]) / 1000)
            if val.endswith("s"):
                return int(float(val[:-1]))
            if val.endswith("m"):
                return int(float(val[:-1]) * 60)
            if val.endswith("min"):
                return int(float(val[:-3]) * 60)
            return int(val)  # assume seconds
        except (ValueError, TypeError):
            return None

    heartbeat = _parse_duration_seconds(heartbeat_str)
    timeout = _parse_duration_seconds(timeout_str)

    if heartbeat is None or timeout is None:
        return findings

    # Heartbeat must be significantly less than network timeout
    if heartbeat >= timeout:
        findings.append(Finding(
            severity=Severity.ERROR,
            keys=["spark.executor.heartbeatInterval", "spark.network.timeout"],
            message=(
                f"heartbeatInterval ({heartbeat_str}) >= network.timeout ({timeout_str}). "
                "Executors will be killed before they can send a heartbeat — "
                "every GC pause longer than the timeout kills the executor."
            ),
            recommendation=(
                "Set heartbeatInterval to at most 1/3 of network.timeout. "
                "E.g., network.timeout=300s with heartbeatInterval=60s."
            ),
            rule="heartbeat-timeout-mismatch",
        ))
    elif heartbeat > timeout * 0.5:
        findings.append(Finding(
            severity=Severity.WARN,
            keys=["spark.executor.heartbeatInterval", "spark.network.timeout"],
            message=(
                f"heartbeatInterval ({heartbeat_str}) is more than half of "
                f"network.timeout ({timeout_str}). A single missed heartbeat + GC pause "
                "could kill a healthy executor."
            ),
            recommendation=(
                "Set heartbeatInterval to at most 1/3 of network.timeout for safety margin."
            ),
            rule="heartbeat-timeout-mismatch",
        ))

    return findings


# ---------------------------------------------------------------------------
# Rule: off-heap enabled but size missing or zero
# ---------------------------------------------------------------------------
@rule("offheap-missing-size")
def check_offheap(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    offheap_enabled = as_bool(conf.get("spark.memory.offHeap.enabled"))
    offheap_size_str = conf.get("spark.memory.offHeap.size")

    if offheap_enabled is True:
        if offheap_size_str is None:
            findings.append(Finding(
                severity=Severity.ERROR,
                keys=["spark.memory.offHeap.enabled", "spark.memory.offHeap.size"],
                message=(
                    "Off-heap memory is enabled but spark.memory.offHeap.size is not set. "
                    "Spark will fail at startup or allocate 0 bytes off-heap, which is useless."
                ),
                recommendation=(
                    "Set spark.memory.offHeap.size to a positive value (e.g., '2g') "
                    "or disable off-heap if you don't need it."
                ),
                rule="offheap-missing-size",
            ))
        else:
            offheap_size = parse_memory(str(offheap_size_str))
            if offheap_size is not None and offheap_size == 0:
                findings.append(Finding(
                    severity=Severity.ERROR,
                    keys=["spark.memory.offHeap.enabled", "spark.memory.offHeap.size"],
                    message=(
                        "Off-heap memory is enabled but size is set to 0. "
                        "This effectively disables off-heap despite the flag being true."
                    ),
                    recommendation=(
                        "Set spark.memory.offHeap.size to a positive value (e.g., '2g') "
                        "or set spark.memory.offHeap.enabled=false."
                    ),
                    rule="offheap-missing-size",
                ))

    return findings


# ---------------------------------------------------------------------------
# Rule: shuffle.partitions set as non-integer string
# ---------------------------------------------------------------------------
@rule("shuffle-partitions-type")
def check_shuffle_partitions_type(config: dict) -> list[Finding]:
    conf = get_spark_conf(config)
    findings: list[Finding] = []

    partitions = conf.get("spark.sql.shuffle.partitions")
    if partitions is None:
        return findings

    val = str(partitions).strip()
    # Check if it's a float string like "200.0" or has units like "200m"
    try:
        int(val)
    except ValueError:
        try:
            float(val)
            findings.append(Finding(
                severity=Severity.WARN,
                keys=["spark.sql.shuffle.partitions"],
                message=(
                    f"shuffle.partitions is set to '{val}' (a float). "
                    "Some Spark submit frameworks stringify config values. "
                    "Spark expects an integer — this may silently fall back to the default 200."
                ),
                recommendation=(
                    f"Set spark.sql.shuffle.partitions to '{int(float(val))}' (integer)."
                ),
                rule="shuffle-partitions-type",
            ))
        except ValueError:
            findings.append(Finding(
                severity=Severity.ERROR,
                keys=["spark.sql.shuffle.partitions"],
                message=(
                    f"shuffle.partitions is set to '{val}' which is not a valid integer. "
                    "Spark will likely fall back to the default or fail."
                ),
                recommendation=(
                    "Set spark.sql.shuffle.partitions to a plain integer (e.g., '200')."
                ),
                rule="shuffle-partitions-type",
            ))

    return findings

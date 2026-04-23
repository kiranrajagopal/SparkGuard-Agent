"""Real verification against Spark infrastructure.

Static re-linting is circular — we check our own output against our own rules.
Real verification means asking Spark itself:

1. spark-submit dry-run: Does Spark accept this config? (catches deprecated keys,
   invalid class names, version-specific settings our rules don't know about)

2. History Server: How did the job ACTUALLY perform? Compare metrics before/after
   a config change — duration, shuffle bytes, GC time, OOM kills.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Any

import urllib.request
import urllib.error


# ---------------------------------------------------------------------------
# 1. spark-submit dry-run — does Spark accept this config?
# ---------------------------------------------------------------------------

@dataclass
class DryRunResult:
    """Result of a spark-submit dry-run validation."""
    accepted: bool                  # did Spark accept the config without errors?
    spark_warnings: list[str]       # warnings Spark itself emitted
    spark_errors: list[str]         # errors Spark reported
    deprecated_keys: list[str]      # keys Spark flagged as deprecated
    unrecognized_keys: list[str]    # keys Spark didn't recognize
    spark_version: str = ""         # detected Spark version

    def format_text(self) -> str:
        if self.accepted and not self.spark_warnings:
            return f"✓ Spark {self.spark_version or '(version unknown)'} accepts this config."

        lines: list[str] = []
        if not self.accepted:
            lines.append(f"✗ Spark rejected this config ({len(self.spark_errors)} error(s)):")
            for e in self.spark_errors:
                lines.append(f"  ERROR: {e}")
        if self.deprecated_keys:
            lines.append(f"⚠ {len(self.deprecated_keys)} deprecated key(s):")
            for k in self.deprecated_keys:
                lines.append(f"  - {k}")
        if self.unrecognized_keys:
            lines.append(f"⚠ {len(self.unrecognized_keys)} unrecognized key(s):")
            for k in self.unrecognized_keys:
                lines.append(f"  - {k}")
        if self.spark_warnings:
            lines.append(f"⚠ {len(self.spark_warnings)} warning(s):")
            for w in self.spark_warnings:
                lines.append(f"  - {w}")
        return "\n".join(lines)

    def as_dict(self) -> dict:
        return {
            "accepted": self.accepted,
            "spark_version": self.spark_version,
            "spark_errors": self.spark_errors,
            "spark_warnings": self.spark_warnings,
            "deprecated_keys": self.deprecated_keys,
            "unrecognized_keys": self.unrecognized_keys,
        }


def find_spark_submit() -> str | None:
    """Locate spark-submit binary. Checks SPARK_HOME, then PATH."""
    spark_home = os.environ.get("SPARK_HOME", "")
    if spark_home:
        candidate = os.path.join(spark_home, "bin", "spark-submit")
        if os.path.isfile(candidate) or os.path.isfile(candidate + ".cmd"):
            return candidate
    return shutil.which("spark-submit")


def dry_run(config_json: str, spark_submit_path: str | None = None) -> DryRunResult:
    """Validate a config by running spark-submit in local mode with a no-op class.

    This doesn't submit a job — it only checks that Spark's own config parser
    accepts every key/value pair. Catches things static rules can't:
    - Deprecated keys across Spark versions
    - Invalid class names (serializers, listeners)
    - Version-specific settings that don't exist in your Spark version
    """
    if spark_submit_path is None:
        spark_submit_path = find_spark_submit()
    if not spark_submit_path:
        return DryRunResult(
            accepted=False,
            spark_warnings=[],
            spark_errors=["spark-submit not found. Set SPARK_HOME or add spark-submit to PATH."],
            deprecated_keys=[],
            unrecognized_keys=[],
        )

    # Parse the config to extract spark.* keys
    from sparkguard.parser import parse_spark_config
    from sparkguard.utils import get_spark_conf

    parsed, _ = parse_spark_config(config_json)
    conf = get_spark_conf(parsed)

    # Build spark-submit command with --conf flags
    # Uses local[1] master and a nonexistent class to trigger config validation
    # without actually running a job
    cmd = [spark_submit_path, "--master", "local[1]", "--verbose"]
    for key, value in conf.items():
        cmd.extend(["--conf", f"{key}={value}"])
    cmd.extend(["--class", "org.apache.spark.sparkguard.DryRunValidator", "nonexistent.jar"])

    # Write a helper script that will fail fast but print config validation
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "SPARK_SUBMIT_OPTS": "-Xmx256m"},
        )
    except FileNotFoundError:
        return DryRunResult(
            accepted=False,
            spark_warnings=[],
            spark_errors=["spark-submit binary not executable."],
            deprecated_keys=[],
            unrecognized_keys=[],
        )
    except subprocess.TimeoutExpired:
        return DryRunResult(
            accepted=False,
            spark_warnings=[],
            spark_errors=["spark-submit timed out after 30s."],
            deprecated_keys=[],
            unrecognized_keys=[],
        )

    stderr = result.stderr or ""
    stdout = result.stdout or ""
    combined = stderr + stdout

    # Parse Spark's output for validation signals
    warnings: list[str] = []
    errors: list[str] = []
    deprecated: list[str] = []
    unrecognized: list[str] = []
    spark_version = ""

    for line in combined.split("\n"):
        line_lower = line.lower()
        # Spark version detection
        if "spark version" in line_lower or "running spark" in line_lower:
            # e.g., "Running Spark version 3.5.1"
            parts = line.strip().split()
            for i, part in enumerate(parts):
                if part.lower() == "version" and i + 1 < len(parts):
                    spark_version = parts[i + 1].strip(",.")
                    break

        # Deprecated key detection
        if "deprecated" in line_lower and "spark." in line:
            deprecated.append(line.strip())
        # Config warnings
        elif "warn" in line_lower and "spark." in line:
            warnings.append(line.strip())
        # Unrecognized config
        elif "not found" in line_lower and "spark." in line:
            unrecognized.append(line.strip())

    # The jar won't exist, so spark-submit will error on ClassNotFound.
    # That's expected — we care about config-level errors BEFORE that point.
    # A config error would show up as "Invalid value" or "IllegalArgumentException"
    for line in combined.split("\n"):
        line_lower = line.lower()
        if any(err in line_lower for err in [
            "invalid value", "illegalargumentexception",
            "invalid spark config", "configuration error",
        ]):
            # Exclude the expected class-not-found error
            if "classnotfound" not in line_lower and "nonexistent.jar" not in line_lower:
                errors.append(line.strip())

    # If the only error is class/jar not found, config was accepted
    accepted = len(errors) == 0

    return DryRunResult(
        accepted=accepted,
        spark_warnings=warnings,
        spark_errors=errors,
        deprecated_keys=deprecated,
        unrecognized_keys=unrecognized,
        spark_version=spark_version,
    )


# ---------------------------------------------------------------------------
# 2. History Server — how did the job ACTUALLY perform?
# ---------------------------------------------------------------------------

@dataclass
class JobMetrics:
    """Metrics from a real Spark job via History Server."""
    app_id: str
    app_name: str = ""
    duration_ms: int = 0
    executor_count: int = 0
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    total_gc_time_ms: int = 0
    total_input_bytes: int = 0
    total_output_bytes: int = 0
    failed_tasks: int = 0
    killed_tasks: int = 0        # often indicates OOM kills
    speculated_tasks: int = 0

    @property
    def duration_str(self) -> str:
        secs = self.duration_ms / 1000
        if secs < 60:
            return f"{secs:.1f}s"
        mins = secs / 60
        if mins < 60:
            return f"{mins:.1f}m"
        return f"{mins / 60:.1f}h"

    @property
    def shuffle_ratio(self) -> str:
        """Shuffle amplification: shuffle_write / input."""
        if self.total_input_bytes == 0:
            return "N/A"
        ratio = self.total_shuffle_write_bytes / self.total_input_bytes
        return f"{ratio:.1f}x"

    def as_dict(self) -> dict:
        return {
            "app_id": self.app_id,
            "app_name": self.app_name,
            "duration_ms": self.duration_ms,
            "duration": self.duration_str,
            "executor_count": self.executor_count,
            "total_shuffle_read_bytes": self.total_shuffle_read_bytes,
            "total_shuffle_write_bytes": self.total_shuffle_write_bytes,
            "total_gc_time_ms": self.total_gc_time_ms,
            "total_input_bytes": self.total_input_bytes,
            "total_output_bytes": self.total_output_bytes,
            "failed_tasks": self.failed_tasks,
            "killed_tasks": self.killed_tasks,
            "speculated_tasks": self.speculated_tasks,
            "shuffle_ratio": self.shuffle_ratio,
        }


@dataclass
class MetricsComparison:
    """Before/after comparison of real job metrics."""
    before: JobMetrics
    after: JobMetrics

    @property
    def duration_change_pct(self) -> float:
        if self.before.duration_ms == 0:
            return 0.0
        return ((self.after.duration_ms - self.before.duration_ms) / self.before.duration_ms) * 100

    @property
    def shuffle_change_pct(self) -> float:
        if self.before.total_shuffle_write_bytes == 0:
            return 0.0
        return ((self.after.total_shuffle_write_bytes - self.before.total_shuffle_write_bytes)
                / self.before.total_shuffle_write_bytes) * 100

    def format_text(self) -> str:
        lines = ["━━━ SparkGuard Metrics Comparison ━━━\n"]

        dur_pct = self.duration_change_pct
        dur_icon = "✓" if dur_pct < 0 else "⚠" if dur_pct > 10 else "~"
        lines.append(f"Duration:  {self.before.duration_str} → {self.after.duration_str}  ({dur_pct:+.1f}%) {dur_icon}")

        shuf_pct = self.shuffle_change_pct
        shuf_icon = "✓" if shuf_pct < 0 else "⚠" if shuf_pct > 20 else "~"
        lines.append(f"Shuffle:   {_fmt_bytes(self.before.total_shuffle_write_bytes)} → {_fmt_bytes(self.after.total_shuffle_write_bytes)}  ({shuf_pct:+.1f}%) {shuf_icon}")

        gc_before = self.before.total_gc_time_ms
        gc_after = self.after.total_gc_time_ms
        lines.append(f"GC time:   {gc_before}ms → {gc_after}ms")

        lines.append(f"Failed:    {self.before.failed_tasks} → {self.after.failed_tasks}")
        lines.append(f"Killed:    {self.before.killed_tasks} → {self.after.killed_tasks}")

        return "\n".join(lines)

    def as_dict(self) -> dict:
        return {
            "before": self.before.as_dict(),
            "after": self.after.as_dict(),
            "duration_change_pct": round(self.duration_change_pct, 1),
            "shuffle_change_pct": round(self.shuffle_change_pct, 1),
        }


def _fmt_bytes(n: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f}PB"


def fetch_job_metrics(
    app_id: str,
    history_server_url: str | None = None,
) -> JobMetrics:
    """Fetch job metrics from Spark History Server REST API.

    Args:
        app_id: Spark application ID (e.g., "application_1234567890_0001")
        history_server_url: Base URL of history server.
            Defaults to SPARK_HISTORY_SERVER env or http://localhost:18080.
    """
    if history_server_url is None:
        history_server_url = os.environ.get(
            "SPARK_HISTORY_SERVER", "http://localhost:18080"
        )

    base = history_server_url.rstrip("/")

    # Fetch application info
    app_data = _history_api_get(f"{base}/api/v1/applications/{app_id}")

    # Fetch all stages to aggregate metrics
    stages = _history_api_get(f"{base}/api/v1/applications/{app_id}/stages")

    # Fetch executors for count
    executors = _history_api_get(f"{base}/api/v1/applications/{app_id}/allexecutors")

    # Aggregate stage-level metrics
    total_shuffle_read = 0
    total_shuffle_write = 0
    total_gc = 0
    total_input = 0
    total_output = 0
    total_failed = 0
    total_killed = 0
    total_speculated = 0

    for stage in stages:
        total_shuffle_read += stage.get("shuffleReadBytes", 0)
        total_shuffle_write += stage.get("shuffleWriteBytes", 0)
        total_gc += stage.get("jvmGcTime", 0)
        total_input += stage.get("inputBytes", 0)
        total_output += stage.get("outputBytes", 0)
        total_failed += stage.get("numFailedTasks", 0)
        total_killed += stage.get("numKilledTasks", 0)
        # Speculated tasks come from task-level details, approximate from stage
        total_speculated += stage.get("numCompleteTasks", 0) - stage.get("numActiveTasks", 0) \
            if stage.get("numCompleteTasks", 0) > stage.get("numTasks", 0) else 0

    # Duration from app attempts
    duration_ms = 0
    attempts = app_data.get("attempts", [])
    if attempts:
        last = attempts[-1]
        duration_ms = last.get("duration", 0)

    return JobMetrics(
        app_id=app_id,
        app_name=app_data.get("name", ""),
        duration_ms=duration_ms,
        executor_count=len([e for e in executors if e.get("id") != "driver"]),
        total_shuffle_read_bytes=total_shuffle_read,
        total_shuffle_write_bytes=total_shuffle_write,
        total_gc_time_ms=total_gc,
        total_input_bytes=total_input,
        total_output_bytes=total_output,
        failed_tasks=total_failed,
        killed_tasks=total_killed,
        speculated_tasks=total_speculated,
    )


def compare_jobs(
    before_app_id: str,
    after_app_id: str,
    history_server_url: str | None = None,
) -> MetricsComparison:
    """Compare two Spark job runs to measure the impact of a config change.

    Args:
        before_app_id: App ID of the run with the old config.
        after_app_id: App ID of the run with the new config.
        history_server_url: History Server URL (default: env or localhost:18080).
    """
    before = fetch_job_metrics(before_app_id, history_server_url)
    after = fetch_job_metrics(after_app_id, history_server_url)
    return MetricsComparison(before=before, after=after)


def _history_api_get(url: str) -> Any:
    """GET a JSON endpoint from Spark History Server."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"History Server API error {e.code} for {url}: {e.read().decode('utf-8', errors='replace')}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Cannot reach History Server at {url}: {e.reason}"
        ) from e

"""Config diff: compare two Spark configs and highlight regressions."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sparkguard.linter import lint
from sparkguard.models import Finding, Severity
from sparkguard.parser import parse_spark_config
from sparkguard.utils import get_spark_conf


@dataclass
class ConfigChange:
    key: str
    kind: str  # "added", "removed", "changed"
    old_value: Any = None
    new_value: Any = None

    def format_text(self) -> str:
        if self.kind == "added":
            return f"  + {self.key}: {self.new_value!r}"
        elif self.kind == "removed":
            return f"  - {self.key}: {self.old_value!r}"
        else:
            return f"  ~ {self.key}: {self.old_value!r} → {self.new_value!r}"


@dataclass
class DiffReport:
    changes: list[ConfigChange] = field(default_factory=list)
    new_findings: list[Finding] = field(default_factory=list)
    resolved_findings: list[Finding] = field(default_factory=list)
    base_path: str = ""
    target_path: str = ""

    @property
    def has_regressions(self) -> bool:
        return any(f.severity in (Severity.ERROR, Severity.WARN) for f in self.new_findings)

    def as_dict(self) -> dict:
        return {
            "base": self.base_path,
            "target": self.target_path,
            "changes": [
                {"key": c.key, "kind": c.kind, "old": c.old_value, "new": c.new_value}
                for c in self.changes
            ],
            "new_findings": [f.as_dict() for f in self.new_findings],
            "resolved_findings": [f.as_dict() for f in self.resolved_findings],
        }


def diff_configs(
    base_raw: str,
    target_raw: str,
    base_path: str = "base",
    target_path: str = "target",
) -> DiffReport:
    """Compare two Spark configs: show changes and find regressions."""
    base_parsed, _ = parse_spark_config(base_raw)
    target_parsed, _ = parse_spark_config(target_raw)

    base_conf = get_spark_conf(base_parsed)
    target_conf = get_spark_conf(target_parsed)

    all_keys = sorted(set(base_conf) | set(target_conf))
    changes: list[ConfigChange] = []

    for key in all_keys:
        in_base = key in base_conf
        in_target = key in target_conf
        if in_base and in_target:
            if str(base_conf[key]) != str(target_conf[key]):
                changes.append(ConfigChange(
                    key=key, kind="changed",
                    old_value=base_conf[key], new_value=target_conf[key],
                ))
        elif in_base:
            changes.append(ConfigChange(key=key, kind="removed", old_value=base_conf[key]))
        else:
            changes.append(ConfigChange(key=key, kind="added", new_value=target_conf[key]))

    # Lint both and diff findings
    base_report = lint(base_raw, config_path=base_path)
    target_report = lint(target_raw, config_path=target_path)

    def _finding_key(f: Finding) -> str:
        return f"{f.rule}::{','.join(sorted(f.keys))}"

    base_keys = {_finding_key(f) for f in base_report.findings}
    target_keys = {_finding_key(f) for f in target_report.findings}

    new_findings = [f for f in target_report.findings if _finding_key(f) not in base_keys]
    resolved_findings = [f for f in base_report.findings if _finding_key(f) not in target_keys]

    return DiffReport(
        changes=changes,
        new_findings=new_findings,
        resolved_findings=resolved_findings,
        base_path=base_path,
        target_path=target_path,
    )


def format_diff_text(report: DiffReport) -> str:
    """Format a DiffReport as human-readable text."""
    lines: list[str] = []
    lines.append(f"━━━ SparkGuard Diff: {report.base_path} → {report.target_path} ━━━\n")

    if not report.changes:
        lines.append("No config changes detected.\n")
    else:
        lines.append(f"{len(report.changes)} config change(s):\n")
        for c in report.changes:
            lines.append(c.format_text())
        lines.append("")

    if report.new_findings:
        lines.append(f"⚠ {len(report.new_findings)} NEW issue(s) introduced:\n")
        for f in report.new_findings:
            lines.append(f.format_text())
            lines.append("")
    else:
        lines.append("✓ No new issues introduced.\n")

    if report.resolved_findings:
        lines.append(f"✓ {len(report.resolved_findings)} issue(s) resolved:\n")
        for f in report.resolved_findings:
            lines.append(f"  [RESOLVED] {f.message}")
        lines.append("")

    return "\n".join(lines)

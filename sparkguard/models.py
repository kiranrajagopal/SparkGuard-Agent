"""Data models for SparkGuard findings."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Severity(enum.Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"


@dataclass
class Finding:
    severity: Severity
    keys: list[str]
    message: str
    recommendation: str
    rule: str  # machine-readable rule id, e.g. "duplicate-key"

    def as_dict(self) -> dict:
        return {
            "severity": self.severity.value,
            "keys": self.keys,
            "message": self.message,
            "recommendation": self.recommendation,
            "rule": self.rule,
        }

    def format_text(self) -> str:
        keys_str = ", ".join(self.keys)
        lines = [
            f"[{self.severity.value}] {self.message}",
            f"  Keys: {keys_str}",
            f"  Fix:  {self.recommendation}",
            f"  Rule: {self.rule}",
        ]
        return "\n".join(lines)


@dataclass
class LintReport:
    findings: list[Finding] = field(default_factory=list)
    config_path: str = ""

    @property
    def error_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.ERROR)

    @property
    def warn_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.WARN)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.INFO)

    def as_dict(self) -> dict:
        return {
            "config_path": self.config_path,
            "summary": {
                "errors": self.error_count,
                "warnings": self.warn_count,
                "info": self.info_count,
            },
            "findings": [f.as_dict() for f in self.findings],
        }

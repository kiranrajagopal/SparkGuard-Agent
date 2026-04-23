"""Top-level linter: wires parser → engine → report."""

from __future__ import annotations

import sparkguard.rules as _rules_module  # noqa: F401 — import triggers @rule registration

from sparkguard.engine import run_all_rules
from sparkguard.formats import parse_config
from sparkguard.models import LintReport
from sparkguard.parser import parse_spark_config


def lint(raw: str, config_path: str = "<stdin>") -> LintReport:
    """Lint a config string in any supported format and return a full report.

    Accepts JSON, Properties (.conf/.properties), YAML, and HOCON.
    Format is auto-detected from config_path extension or content.
    """
    config, parse_findings = parse_config(raw, filename=config_path)
    rule_findings = run_all_rules(config)

    report = LintReport(
        findings=parse_findings + rule_findings,
        config_path=config_path,
    )
    return report


def lint_json(raw_json: str, config_path: str = "<stdin>") -> LintReport:
    """Lint a raw JSON string specifically (preserves original JSON-only API)."""
    config, dup_findings = parse_spark_config(raw_json)
    rule_findings = run_all_rules(config)

    report = LintReport(
        findings=dup_findings + rule_findings,
        config_path=config_path,
    )
    return report

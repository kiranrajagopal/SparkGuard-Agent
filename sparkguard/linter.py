"""Top-level linter: wires parser → engine → report."""

from __future__ import annotations

import sparkguard.rules as _rules_module  # noqa: F401 — import triggers @rule registration

from sparkguard.engine import run_all_rules
from sparkguard.models import LintReport
from sparkguard.parser import parse_spark_config


def lint(raw_json: str, config_path: str = "<stdin>") -> LintReport:
    """Lint a raw JSON string and return a full report."""
    config, dup_findings = parse_spark_config(raw_json)
    rule_findings = run_all_rules(config)

    report = LintReport(
        findings=dup_findings + rule_findings,
        config_path=config_path,
    )
    return report

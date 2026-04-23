"""Rule engine: registry + runner for config lint rules."""

from __future__ import annotations

from typing import Callable, Protocol

from sparkguard.models import Finding

# Type alias for a rule function
RuleFunc = Callable[[dict], list[Finding]]

_RULES: list[tuple[str, RuleFunc]] = []


def rule(name: str):
    """Decorator to register a lint rule."""
    def decorator(fn: RuleFunc) -> RuleFunc:
        _RULES.append((name, fn))
        return fn
    return decorator


def run_all_rules(config: dict) -> list[Finding]:
    """Execute every registered rule against *config* and return all findings."""
    findings: list[Finding] = []
    for _name, fn in _RULES:
        findings.extend(fn(config))
    return findings

"""JSON parser that detects duplicate keys (JSON spec allows them, Python dict silently last-wins)."""

from __future__ import annotations

import json
from typing import Any

from sparkguard.models import Finding, Severity


class DuplicateKeyError:
    """Tracks a single duplicate-key occurrence."""

    __slots__ = ("path", "key", "values")

    def __init__(self, path: str, key: str, first_value: Any, second_value: Any):
        self.path = path
        self.key = key
        self.values = [first_value, second_value]


def parse_spark_config(raw: str) -> tuple[dict, list[Finding]]:
    """Parse a JSON string, returning (parsed_dict, list_of_duplicate_key_findings).

    The parsed dict uses last-wins semantics (standard Python behaviour) so that
    the caller sees what Spark would actually see. But every duplicate key is
    recorded as an ERROR finding.
    """
    duplicates: list[DuplicateKeyError] = []
    _path_stack: list[str] = []

    def _object_pairs_hook(pairs: list[tuple[str, Any]]) -> dict:
        seen: dict[str, Any] = {}
        current_path = ".".join(_path_stack) if _path_stack else "<root>"
        for key, value in pairs:
            if key in seen:
                duplicates.append(DuplicateKeyError(current_path, key, seen[key], value))
            seen[key] = value
        return seen

    parsed = json.loads(raw, object_pairs_hook=_object_pairs_hook)

    findings: list[Finding] = []
    for dup in duplicates:
        # Determine if the overwrite actually changed the value
        if len(dup.values) >= 2 and dup.values[0] != dup.values[-1]:
            sev = Severity.ERROR
            impact = (
                f"Value changed from {dup.values[0]!r} to {dup.values[-1]!r} — "
                "last-wins semantics silently override the earlier value."
            )
        else:
            sev = Severity.WARN
            impact = "Same value repeated — harmless but suggests a copy-paste issue."

        findings.append(
            Finding(
                severity=sev,
                keys=[dup.key],
                message=f"Duplicate key '{dup.key}' in JSON object at {dup.path}. {impact}",
                recommendation=(
                    f"Remove the duplicate '{dup.key}' entry. Keep only the intended value."
                ),
                rule="duplicate-key",
            )
        )

    return parsed, findings

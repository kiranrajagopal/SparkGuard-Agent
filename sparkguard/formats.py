"""Multi-format config parser.

Detects and parses Spark configs in JSON, Properties, YAML, and HOCON formats.
Also extracts embedded Spark configs from script files (.ps1, .py, .sh, .scala).
Returns the same (dict, list[Finding]) interface regardless of input format.

Format support:
  - JSON:       Built-in, with duplicate key detection.
  - Properties: Built-in (spark-defaults.conf / .properties files).
  - YAML:       Requires PyYAML (`pip install sparkguard[yaml]`).
  - HOCON:      Requires pyhocon (`pip install sparkguard[hocon]`).
  - Scripts:    Extracts embedded JSON from .ps1, .py, .sh, .scala files.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from sparkguard.models import Finding, Severity
from sparkguard.parser import parse_spark_config


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

_EXT_MAP: dict[str, str] = {
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".properties": "properties",
    ".conf": "properties",        # spark-defaults.conf
    ".hocon": "hocon",
}

# Script extensions that may contain embedded JSON Spark configs
_SCRIPT_EXTS: set[str] = {".ps1", ".py", ".sh", ".bash", ".scala", ".sc", ".rb", ".pl"}


def detect_format(raw: str, filename: str = "") -> str:
    """Detect config format from file extension, then content sniffing.

    Returns one of: 'json', 'yaml', 'properties', 'hocon', 'script'.
    """
    # 1. File extension (most reliable)
    if filename:
        ext = os.path.splitext(filename)[1].lower()
        if ext in _SCRIPT_EXTS:
            return "script"
        if ext in _EXT_MAP:
            return _EXT_MAP[ext]

    stripped = raw.strip()

    # 2. Content sniffing
    # JSON: starts with { or [
    if stripped.startswith("{") or stripped.startswith("["):
        return "json"

    # HOCON: has include statements, unquoted keys with {, or ${substitution}
    if re.search(r"(?m)^\s*include\s", stripped) or "${" in stripped:
        return "hocon"

    # Properties: lines matching `spark.key.name value` or `spark.key=value`
    lines = [l.strip() for l in stripped.split("\n") if l.strip() and not l.strip().startswith("#")]
    if lines and all(
        re.match(r"^spark\.\S+\s*[=\s]\s*\S", l) or re.match(r"^\w+\.\S+\s*[=\s]\s*\S", l)
        for l in lines[:10]  # check first 10 non-comment lines
    ):
        return "properties"

    # YAML: contains `:` separators without `{` braces (and not properties-style)
    if ":" in stripped and not stripped.startswith("{"):
        return "yaml"

    # Default fallback: try JSON
    return "json"


# ---------------------------------------------------------------------------
# Properties parser (spark-defaults.conf format)
# ---------------------------------------------------------------------------

def _parse_properties(raw: str) -> tuple[dict, list[Finding]]:
    """Parse spark-defaults.conf / .properties format.

    Format:
        spark.executor.memory  8g
        spark.driver.memory=4g
        # comments start with #
    """
    findings: list[Finding] = []
    seen_keys: dict[str, str] = {}
    result: dict[str, Any] = {}

    for line_num, line in enumerate(raw.split("\n"), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("!"):
            continue

        # Split on first `=` or first whitespace
        match = re.match(r"^(\S+)\s*[=\s]\s*(.*)", stripped)
        if not match:
            findings.append(Finding(
                severity=Severity.WARN,
                keys=[],
                message=f"Unparseable line {line_num}: {stripped!r}",
                recommendation="Check the line format. Expected: key=value or key value",
                rule="properties-parse-error",
            ))
            continue

        key, value = match.group(1), match.group(2).strip()

        # Detect duplicate keys (properties files can have them too)
        if key in seen_keys:
            old_val = seen_keys[key]
            if old_val != value:
                findings.append(Finding(
                    severity=Severity.ERROR,
                    keys=[key],
                    message=(
                        f"Duplicate key '{key}' in properties file. "
                        f"Value changed from {old_val!r} to {value!r} — last-wins."
                    ),
                    recommendation=f"Remove the duplicate '{key}' entry. Keep only the intended value.",
                    rule="duplicate-key",
                ))
            else:
                findings.append(Finding(
                    severity=Severity.WARN,
                    keys=[key],
                    message=f"Duplicate key '{key}' in properties file. Same value repeated.",
                    recommendation=f"Remove the duplicate '{key}' entry.",
                    rule="duplicate-key",
                ))

        seen_keys[key] = value
        result[key] = value

    return result, findings


# ---------------------------------------------------------------------------
# YAML parser (requires PyYAML)
# ---------------------------------------------------------------------------

def _parse_yaml(raw: str) -> tuple[dict, list[Finding]]:
    """Parse YAML config. Requires PyYAML.

    Handles both flat and nested YAML:
        Flat:     spark.executor.memory: 8g
        Nested:   spark:
                    executor:
                      memory: 8g
    """
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "PyYAML is required for YAML config files.\n"
            "Install with: pip install 'sparkguard[yaml]'"
        )

    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML: {e}") from e

    if not isinstance(parsed, dict):
        raise ValueError(f"Expected a YAML mapping, got {type(parsed).__name__}")

    # Flatten nested YAML into dotted keys if needed
    # e.g., {spark: {executor: {memory: "8g"}}} → {"spark.executor.memory": "8g"}
    flat = _flatten_dict(parsed)

    # Check if it has spark keys directly, or if it's a wrapper
    if any(str(k).startswith("spark.") for k in flat):
        # Already flat or was nested spark.*
        return flat, []

    # Could be a wrapper like {"conf": {"spark...": ...}} — return as-is for get_spark_conf
    return parsed, []


def _flatten_dict(d: dict, prefix: str = "") -> dict:
    """Flatten a nested dict into dotted keys.

    Only flattens dicts (not lists or scalars).
    Stops flattening when a value is not a dict.
    """
    result: dict[str, Any] = {}
    for key, value in d.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            result.update(_flatten_dict(value, full_key))
        else:
            result[full_key] = value
    return result


# ---------------------------------------------------------------------------
# HOCON parser (requires pyhocon)
# ---------------------------------------------------------------------------

def _parse_hocon(raw: str) -> tuple[dict, list[Finding]]:
    """Parse HOCON (Typesafe Config) format. Requires pyhocon.

    HOCON is a superset of JSON with:
    - Unquoted keys and values
    - Comments (# and //)
    - ${substitution} references
    - include statements
    """
    try:
        from pyhocon import ConfigFactory
    except ImportError:
        raise ImportError(
            "pyhocon is required for HOCON config files.\n"
            "Install with: pip install 'sparkguard[hocon]'"
        )

    try:
        config = ConfigFactory.parse_string(raw)
        # pyhocon returns a ConfigTree; convert to plain dict
        parsed = _hocon_to_dict(config)
    except Exception as e:
        raise ValueError(f"Invalid HOCON: {e}") from e

    # Flatten and return
    flat = _flatten_dict(parsed)
    if any(str(k).startswith("spark.") for k in flat):
        return flat, []
    return parsed, []


def _hocon_to_dict(config) -> dict:
    """Convert a pyhocon ConfigTree to a plain dict recursively."""
    result: dict[str, Any] = {}
    for key in config:
        value = config[key]
        if hasattr(value, "__iter__") and hasattr(value, "keys"):
            result[key] = _hocon_to_dict(value)
        elif isinstance(value, list):
            result[key] = list(value)
        else:
            result[key] = value
    return result


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def parse_config(raw: str, filename: str = "") -> tuple[dict, list[Finding]]:
    """Parse a Spark config in any supported format.

    Auto-detects format from file extension or content.
    Returns (parsed_dict, findings) — same interface as parse_spark_config.

    For JSON: full duplicate key detection.
    For Properties: duplicate key detection.
    For YAML/HOCON: structural parsing (duplicate detection depends on library).
    For Scripts (.ps1, .py, .sh, .scala): extracts embedded JSON Spark config.
    """
    fmt = detect_format(raw, filename)

    if fmt == "script":
        return _extract_json_from_script(raw, filename)
    elif fmt == "json":
        return parse_spark_config(raw)
    elif fmt == "properties":
        return _parse_properties(raw)
    elif fmt == "yaml":
        return _parse_yaml(raw)
    elif fmt == "hocon":
        return _parse_hocon(raw)
    else:
        # Fallback: try JSON
        return parse_spark_config(raw)


def _extract_json_from_script(raw: str, filename: str = "") -> tuple[dict, list[Finding]]:
    """Extract embedded JSON Spark config from a script file.

    Scans for JSON objects ({...}) in the file and returns the first one that
    contains Spark configuration keys. Works with:
      - PowerShell here-strings:  @' ... '@  or @" ... "@
      - Python triple-quote strings: '''...''' or \"\"\"...\"\"\"
      - Shell heredocs: <<EOF ... EOF
      - Any JSON blob containing "spark." or "conf" keys
    """
    from sparkguard.utils import get_spark_conf

    # Strategy: find all JSON-like blobs, parse each, keep the one with spark config
    candidates = _find_json_blobs(raw)

    for blob in candidates:
        try:
            parsed, findings = parse_spark_config(blob)
            conf = get_spark_conf(parsed)
            if conf:
                return parsed, findings
        except (json.JSONDecodeError, ValueError):
            continue

    raise ValueError(
        f"No embedded Spark config found in {filename or 'script'}. "
        f"Looked for JSON objects containing spark.* keys or conf/sparkConf blocks."
    )


def _find_json_blobs(raw: str) -> list[str]:
    """Find all JSON object candidates in a script file.

    Uses brace-matching to extract top-level {...} blocks, then returns them
    sorted by size (largest first — the full Livy payload is usually the biggest).
    """
    blobs: list[str] = []
    i = 0
    while i < len(raw):
        if raw[i] == "{":
            # Try to match braces
            depth = 0
            start = i
            in_string = False
            escape = False
            for j in range(i, len(raw)):
                ch = raw[j]
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == '"' and not escape:
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        blob = raw[start:j + 1]
                        # Quick sanity: must contain a quote (JSON keys are quoted)
                        if '"' in blob:
                            blobs.append(blob)
                        i = j + 1
                        break
            else:
                # Unmatched brace, skip
                i += 1
        else:
            i += 1

    # Sort by length descending — the full Livy payload is usually the largest
    blobs.sort(key=len, reverse=True)
    return blobs


def serialize_config(config: dict, fmt: str = "json") -> str:
    """Serialize a config dict back to the specified format.

    Currently supports: json, properties.
    YAML/HOCON output falls back to JSON.
    """
    import json as _json

    if fmt == "properties":
        return _serialize_properties(config)

    # Default: JSON (works for yaml/hocon fallback too)
    return _json.dumps(config, indent=4)


def _serialize_properties(config: dict) -> str:
    """Serialize a flat dict to spark-defaults.conf format."""
    from sparkguard.utils import get_spark_conf

    conf = get_spark_conf(config) if not any(str(k).startswith("spark.") for k in config) else config
    lines: list[str] = []
    for key in sorted(conf):
        lines.append(f"{key}  {conf[key]}")
    return "\n".join(lines) + "\n"

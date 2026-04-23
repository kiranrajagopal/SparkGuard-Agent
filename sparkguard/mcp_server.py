"""SparkGuard MCP Server — exposes lint/fix/diff/generate as tools for AI agents."""

from __future__ import annotations

import json
import sys
from typing import Any

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent

    HAS_MCP = True
except ImportError:
    HAS_MCP = False

from sparkguard.linter import lint
from sparkguard.fixer import fix_config
from sparkguard.diff import diff_configs


def _make_tools() -> list["Tool"]:
    """Define the MCP tools SparkGuard exposes."""
    return [
        Tool(
            name="sparkguard_lint",
            description=(
                "Lint a Spark submit JSON config for misconfigurations, duplicate keys, "
                "contradictions, and anti-patterns. Returns structured findings with "
                "severity, affected keys, explanation, and fix recommendation."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "config_json": {
                        "type": "string",
                        "description": "The raw JSON string of the Spark submit config to lint.",
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["ERROR", "WARN", "INFO"],
                        "default": "INFO",
                        "description": "Minimum severity to include in results.",
                    },
                },
                "required": ["config_json"],
            },
        ),
        Tool(
            name="sparkguard_fix",
            description=(
                "Auto-fix safe issues in a Spark submit JSON config. Removes duplicate "
                "keys, enables AQE when sub-settings exist, adds KryoSerializer, fixes "
                "memory overhead, and more. Returns the fixed JSON and a list of changes made."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "config_json": {
                        "type": "string",
                        "description": "The raw JSON string of the Spark config to fix.",
                    },
                },
                "required": ["config_json"],
            },
        ),
        Tool(
            name="sparkguard_diff",
            description=(
                "Compare two Spark configs. Shows every key added, removed, or changed, "
                "plus new issues introduced (regressions) and issues resolved."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "base_config_json": {
                        "type": "string",
                        "description": "The 'before' Spark config JSON.",
                    },
                    "target_config_json": {
                        "type": "string",
                        "description": "The 'after' Spark config JSON.",
                    },
                },
                "required": ["base_config_json", "target_config_json"],
            },
        ),
        Tool(
            name="sparkguard_generate",
            description=(
                "Generate an optimized Spark config from a workload profile. Provide "
                "details about dataset size, job type, cluster specs, etc. and get a "
                "tuned config with explanations."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_size_gb": {
                        "type": "string",
                        "enum": ["< 1 GB", "1-10 GB", "10-100 GB", "100-500 GB", "500 GB - 1 TB", "> 1 TB"],
                        "description": "Approximate input dataset size.",
                    },
                    "job_type": {
                        "type": "string",
                        "enum": ["ETL / transform", "Aggregation / reporting", "ML training", "Streaming", "SQL analytics"],
                        "description": "Type of Spark job.",
                    },
                    "has_joins": {"type": "boolean", "default": True, "description": "Does the job perform joins?"},
                    "has_skew": {"type": "boolean", "default": False, "description": "Is there data skew?"},
                    "uses_caching": {"type": "boolean", "default": False, "description": "Does the job cache DataFrames?"},
                    "uses_pyspark": {"type": "boolean", "default": True, "description": "PySpark (true) or Scala (false)?"},
                    "uses_udfs": {"type": "boolean", "default": False, "description": "Uses Python/Pandas UDFs?"},
                    "cluster_node_memory_gb": {"type": "string", "default": "64 GB", "description": "Memory per worker node."},
                    "cluster_node_cores": {"type": "string", "default": "16", "description": "CPU cores per worker node."},
                    "cluster_node_count": {"type": "string", "default": "10-20", "description": "Number of worker nodes."},
                    "output_format": {
                        "type": "string",
                        "enum": ["Parquet", "Delta", "ORC", "CSV/JSON", "Database sink"],
                        "default": "Parquet",
                        "description": "Output format.",
                    },
                },
                "required": ["dataset_size_gb", "job_type"],
            },
        ),
    ]


async def _handle_call(name: str, arguments: dict[str, Any]) -> list["TextContent"]:
    """Route an MCP tool call to the appropriate SparkGuard function."""
    from sparkguard.models import Severity

    if name == "sparkguard_lint":
        config_json = arguments["config_json"]
        min_severity = arguments.get("severity", "INFO")
        report = lint(config_json)
        severity_order = {Severity.ERROR: 0, Severity.WARN: 1, Severity.INFO: 2}
        min_ord = severity_order[Severity[min_severity]]
        report.findings = [f for f in report.findings if severity_order[f.severity] <= min_ord]
        return [TextContent(type="text", text=json.dumps(report.as_dict(), indent=2))]

    elif name == "sparkguard_fix":
        config_json = arguments["config_json"]
        fixed_json, descriptions = fix_config(config_json)
        result = {
            "fixed_config": json.loads(fixed_json),
            "changes_applied": descriptions,
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    elif name == "sparkguard_diff":
        base = arguments["base_config_json"]
        target = arguments["target_config_json"]
        report = diff_configs(base, target, "base", "target")
        return [TextContent(type="text", text=json.dumps(report.as_dict(), indent=2))]

    elif name == "sparkguard_generate":
        from sparkguard.generator import (
            collect_profile_from_answers,
            format_config_json,
            generate_config,
        )
        # Map MCP arguments to the answer format the generator expects
        answers: dict[str, str] = {}
        field_map = {
            "dataset_size_gb": "dataset_size_gb",
            "job_type": "job_type",
            "has_joins": "has_joins",
            "has_skew": "has_skew",
            "uses_caching": "uses_caching",
            "uses_pyspark": "uses_pyspark",
            "uses_udfs": "uses_udfs",
            "cluster_node_memory_gb": "cluster_node_memory_gb",
            "cluster_node_cores": "cluster_node_cores",
            "cluster_node_count": "cluster_node_count",
            "output_format": "output_format",
        }
        for mcp_key, gen_key in field_map.items():
            if mcp_key in arguments:
                val = arguments[mcp_key]
                # Convert booleans to Yes/No strings for the generator
                if isinstance(val, bool):
                    val = "Yes" if val else "No"
                answers[gen_key] = str(val)

        profile = collect_profile_from_answers(answers)
        conf = generate_config(profile)
        result = {
            "config": conf,
            "config_json": json.loads(format_config_json(conf)),
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def run_mcp_server() -> None:
    """Start the SparkGuard MCP server over stdio."""
    if not HAS_MCP:
        print(
            "Error: MCP SDK not installed. Install with: pip install 'sparkguard[mcp]'",
            file=sys.stderr,
        )
        sys.exit(1)

    server = Server("sparkguard")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return _make_tools()

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        return await _handle_call(name, arguments)

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

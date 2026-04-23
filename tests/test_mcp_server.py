"""Tests for SparkGuard MCP server tool definitions and handler routing."""

from __future__ import annotations

import json
import pytest

from sparkguard.mcp_server import HAS_MCP, _handle_call

pytestmark = pytest.mark.skipif(not HAS_MCP, reason="MCP SDK not installed")


class TestMCPToolDefinitions:
    """Verify tool definitions are well-formed."""

    def test_tools_have_required_fields(self):
        from sparkguard.mcp_server import _make_tools
        tools = _make_tools()
        assert len(tools) == 4
        names = {t.name for t in tools}
        assert names == {"sparkguard_lint", "sparkguard_fix", "sparkguard_diff", "sparkguard_generate"}

    def test_each_tool_has_schema(self):
        from sparkguard.mcp_server import _make_tools
        for tool in _make_tools():
            assert tool.inputSchema is not None
            assert "properties" in tool.inputSchema
            assert "required" in tool.inputSchema


class TestMCPHandlers:
    """Test the handler logic (bypassing MCP transport)."""

    GOOD_CONFIG = json.dumps({
        "sparkConf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.executor.memory": "8g",
        }
    })

    BAD_CONFIG = json.dumps({
        "sparkConf": {
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
    })

    @pytest.mark.asyncio
    async def test_lint_clean_config(self):
        result = await _handle_call("sparkguard_lint", {"config_json": self.GOOD_CONFIG})
        data = json.loads(result[0].text)
        assert data["error_count"] == 0

    @pytest.mark.asyncio
    async def test_lint_bad_config(self):
        result = await _handle_call("sparkguard_lint", {"config_json": self.BAD_CONFIG})
        data = json.loads(result[0].text)
        assert data["error_count"] > 0

    @pytest.mark.asyncio
    async def test_fix_returns_changes(self):
        result = await _handle_call("sparkguard_fix", {"config_json": self.BAD_CONFIG})
        data = json.loads(result[0].text)
        assert "fixed_config" in data
        assert "changes_applied" in data

    @pytest.mark.asyncio
    async def test_diff_detects_changes(self):
        result = await _handle_call("sparkguard_diff", {
            "base_config_json": self.GOOD_CONFIG,
            "target_config_json": self.BAD_CONFIG,
        })
        data = json.loads(result[0].text)
        assert "changes" in data

    @pytest.mark.asyncio
    async def test_generate_produces_config(self):
        result = await _handle_call("sparkguard_generate", {
            "dataset_size_gb": "10-100 GB",
            "job_type": "ETL / transform",
        })
        data = json.loads(result[0].text)
        assert "config" in data

    @pytest.mark.asyncio
    async def test_unknown_tool(self):
        result = await _handle_call("sparkguard_nope", {})
        assert "Unknown tool" in result[0].text

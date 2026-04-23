"""Tests for SparkGuard agent reasoning layer."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from sparkguard.agent import (
    AgentAnalysis,
    AgentConfig,
    analyze,
    analyze_offline,
    _build_user_prompt,
    _observe_act_check,
    _parse_llm_response,
)


# ---- Test configs ----

CLEAN_CONFIG = json.dumps({
    "sparkConf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.executor.memory": "8g",
    }
})

BAD_CONFIG = json.dumps({
    "sparkConf": {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.executor.memory": "32g",
        "spark.executor.memoryOverhead": "256m",
        "spark.executor.instances": "200",
    }
})

# Duplicate-key config (raw string, not via json.dumps)
DUP_KEY_CONFIG = """{
    "sparkConf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
}"""


class TestObserveActVerify:
    """Test the core agent loop — the part a chat LLM can't do."""

    def test_bad_config_gets_fixed(self):
        loop = _observe_act_check(BAD_CONFIG, auto_fix=True)
        assert loop["before_count"] > 0
        assert loop["actions_taken"]  # at least one fix applied
        assert loop["after_count"] < loop["before_count"]  # fixes reduced issues
        assert loop["fixed_config"] is not None
        # Verify the fixed config is valid JSON
        json.loads(loop["fixed_config"])

    def test_no_fix_mode_observes_only(self):
        loop = _observe_act_check(BAD_CONFIG, auto_fix=False)
        assert loop["before_count"] > 0
        assert loop["actions_taken"] == []
        assert loop["fixed_config"] is None
        assert loop["after_count"] == loop["before_count"]

    def test_verify_catches_residuals(self):
        loop = _observe_act_check(BAD_CONFIG, auto_fix=True)
        # Some issues can't be auto-fixed (e.g., speculation-risk)
        assert loop["residual_findings"] is not None
        # Residual count matches after_count
        assert len(loop["residual_findings"]) == loop["after_count"]

    def test_duplicate_key_config_fixed(self):
        loop = _observe_act_check(DUP_KEY_CONFIG, auto_fix=True)
        assert any("duplicate" in a.lower() or "Removed" in a for a in loop["actions_taken"])
        assert loop["after_count"] < loop["before_count"]


class TestAnalyzeOffline:
    """Test the offline (no-LLM) analysis path with act loop."""

    def test_clean_config_info_only(self):
        result = analyze_offline(CLEAN_CONFIG)
        assert isinstance(result, AgentAnalysis)
        assert result.priority_order  # has some findings (INFO recs)

    def test_bad_config_takes_action(self):
        result = analyze_offline(BAD_CONFIG)
        assert result.actions_taken  # agent DID something
        assert result.before_count > result.after_count  # fixes worked
        assert result.fixed_config is not None
        assert "Applied" in result.summary  # summary mentions action

    def test_bad_config_no_fix_mode(self):
        result = analyze_offline(BAD_CONFIG, auto_fix=False)
        assert result.actions_taken == []
        assert result.fixed_config is None
        assert result.before_count == result.after_count

    def test_errors_first_in_priority(self):
        result = analyze_offline(BAD_CONFIG)
        from sparkguard.linter import lint
        from sparkguard.models import Severity
        report = lint(BAD_CONFIG)
        error_rules = {f.rule for f in report.findings if f.severity == Severity.ERROR}
        if error_rules:
            assert result.priority_order[0] in error_rules

    def test_duplicate_key_root_cause_detected(self):
        result = analyze_offline(DUP_KEY_CONFIG)
        root_text = " ".join(result.root_causes).lower()
        assert "duplicate" in root_text

    def test_format_text_shows_actions(self):
        result = analyze_offline(BAD_CONFIG)
        text = result.format_text()
        assert "SparkGuard Agent Analysis" in text
        assert "Actions Taken" in text
        assert "Before:" in text
        assert "After:" in text
        assert "Root Causes" in text

    def test_as_dict_includes_action_fields(self):
        result = analyze_offline(BAD_CONFIG)
        d = result.as_dict()
        assert "actions_taken" in d
        assert "before_count" in d
        assert "after_count" in d
        assert d["before_count"] > d["after_count"]


class TestBuildUserPrompt:
    """Test prompt construction."""

    def test_includes_config_and_findings(self):
        from sparkguard.linter import lint
        report = lint(BAD_CONFIG)
        prompt = _build_user_prompt(BAD_CONFIG, report)
        assert "spark.sql.adaptive.enabled" in prompt
        assert "Lint Findings" in prompt

    def test_includes_action_context(self):
        from sparkguard.linter import lint
        report = lint(BAD_CONFIG)
        prompt = _build_user_prompt(
            BAD_CONFIG, report,
            actions_taken=["Enabled AQE", "Bumped overhead"],
            after_count=3,
            residual_findings=report.findings[:1],
        )
        assert "Actions Already Taken" in prompt
        assert "Enabled AQE" in prompt
        assert "Remaining Issues" in prompt

    def test_includes_code_context(self):
        from sparkguard.linter import lint
        report = lint(BAD_CONFIG)
        prompt = _build_user_prompt(BAD_CONFIG, report, code_context="df.repartition(1000)")
        assert "df.repartition(1000)" in prompt
        assert "Code Context" in prompt


class TestParseLLMResponse:
    """Test response parsing robustness."""

    def test_plain_json(self):
        raw = '{"summary": "test", "root_causes": []}'
        result = _parse_llm_response(raw)
        assert result["summary"] == "test"

    def test_fenced_json(self):
        raw = '```json\n{"summary": "test"}\n```'
        result = _parse_llm_response(raw)
        assert result["summary"] == "test"


class TestAnalyzeWithLLM:
    """Test the LLM-powered path with mocked API calls."""

    MOCK_LLM_RESPONSE = json.dumps({
        "summary": "AQE was auto-fixed. Remaining: speculation risk and high executor count.",
        "root_causes": ["Copy-paste error disabled AQE"],
        "priority_order": ["speculation-risk", "dynamic-alloc-off-high-executors"],
        "fix_plan": "1. Enable dynamic allocation\n2. Ensure idempotent writes for speculation",
        "risk_assessment": "200 fixed executors waste money during low-load periods.",
        "config_context": "Large ETL job with 200 executors and 32g memory.",
    })

    @patch("sparkguard.agent._call_llm", return_value=MOCK_LLM_RESPONSE)
    def test_analyze_with_mock_llm(self, mock_llm):
        config = AgentConfig(api_key="test-key", model="test-model")
        result = analyze(BAD_CONFIG, agent_config=config)
        assert result.root_causes
        assert result.actions_taken  # agent acted BEFORE calling LLM
        assert result.before_count > 0
        assert mock_llm.called

    @patch("sparkguard.agent._call_llm", return_value=MOCK_LLM_RESPONSE)
    def test_truly_clean_config_skips_llm(self, mock_llm):
        truly_clean = json.dumps({"sparkConf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.executor.memory": "8g",
        }})
        config = AgentConfig(api_key="test-key", model="test-model")
        result = analyze(truly_clean, agent_config=config)
        assert "clean" in result.summary.lower() or "no issues" in result.summary.lower()
        assert not mock_llm.called

    def test_analyze_no_api_key_raises(self):
        config = AgentConfig(api_key="", model="test")
        with pytest.raises(RuntimeError, match="No API key"):
            analyze(BAD_CONFIG, agent_config=config)

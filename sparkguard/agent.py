"""Agentic reasoning layer — observes, acts, and checks.

What makes this an agent (vs "paste into ChatGPT"):

1. DETERMINISTIC ANALYSIS — duplicate key detection via object_pairs_hook, memory math,
   timeout ratio validation. LLMs get these wrong. We don't.

2. MULTI-STEP ACT LOOP — lint → auto-fix → re-lint (consistency check) → report delta.
   The agent takes action, not just advice.

3. REAL VERIFICATION (when available) — spark-submit dry-run validates Spark itself
   accepts the config. History Server metrics compare actual job performance before/after.
   See sparkguard/verify.py for the runtime verification layer.

4. TOOL COMPOSITION — via MCP, other agents delegate Spark expertise to us.
   They don't need to know Spark config semantics — they call a specialist.

5. LLM AS REASONING LAYER (optional) — the LLM doesn't replace the rules, it reasons
   ABOUT the deterministic findings: why they happened, how they connect, what to do first.

HONEST LIMITATION: The re-lint step is a consistency check, not runtime proof.
To prove a config change actually improved performance, you need:
  - sparkguard verify --dry-run config.json   (does Spark accept it?)
  - sparkguard verify --compare app-1 app-2   (did it actually get faster?)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

from sparkguard.linter import lint
from sparkguard.fixer import fix_config
from sparkguard.models import Finding, LintReport, Severity
from sparkguard.parser import parse_spark_config
from sparkguard.utils import get_spark_conf

# ---------------------------------------------------------------------------
# LLM provider abstraction — supports OpenAI-compatible APIs
# ---------------------------------------------------------------------------

@dataclass
class AgentConfig:
    """Configuration for the LLM backend."""
    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-4o-mini"
    max_tokens: int = 2048
    temperature: float = 0.2

    @classmethod
    def from_env(cls) -> "AgentConfig":
        """Build config from environment variables."""
        return cls(
            api_key=os.environ.get("SPARKGUARD_API_KEY", os.environ.get("OPENAI_API_KEY", "")),
            base_url=os.environ.get("SPARKGUARD_BASE_URL", "https://api.openai.com/v1"),
            model=os.environ.get("SPARKGUARD_MODEL", "gpt-4o-mini"),
        )


@dataclass
class AgentAnalysis:
    """The agent's reasoned analysis of a config."""
    summary: str                           # 2-3 sentence executive summary
    root_causes: list[str]                 # underlying issues, not just symptoms
    priority_order: list[str]              # finding rules in order of fix priority
    fix_plan: str                          # step-by-step plain-English fix plan
    risk_assessment: str                   # what happens if you don't fix this
    config_context: str                    # what the agent infers about the workload
    raw_findings: list[Finding] = field(default_factory=list)
    # --- Action results (the part a chat LLM can't do) ---
    actions_taken: list[str] = field(default_factory=list)   # what the agent DID
    fixed_config: str | None = None        # the repaired config JSON (if acted)
    before_count: int = 0                  # findings before fix
    after_count: int = 0                   # findings after fix (verify step)
    residual_findings: list[Finding] = field(default_factory=list)  # what's still broken

    def format_text(self) -> str:
        lines = [
            "━━━ SparkGuard Agent Analysis ━━━\n",
            f"Summary: {self.summary}\n",
        ]

        # Show what the agent DID, not just what it found
        if self.actions_taken:
            lines.append(f"Actions Taken ({len(self.actions_taken)}):")
            for i, action in enumerate(self.actions_taken, 1):
                lines.append(f"  ✓ {i}. {action}")
            lines.append(f"\n  Before: {self.before_count} finding(s) → After: {self.after_count} finding(s)")
            if self.residual_findings:
                lines.append(f"\n  ⚠ {len(self.residual_findings)} issue(s) remain (require manual fix):")
                for f in self.residual_findings:
                    lines.append(f"    [{f.severity.value}] {f.rule}: {f.message}")
            lines.append("")

        lines.append("Root Causes:")
        for i, cause in enumerate(self.root_causes, 1):
            lines.append(f"  {i}. {cause}")

        lines.append(f"\nWorkload Context: {self.config_context}\n")
        lines.append(f"Risk if Unfixed: {self.risk_assessment}\n")
        lines.append("Fix Plan (priority order):")
        lines.append(self.fix_plan)
        lines.append("")

        return "\n".join(lines)

    def as_dict(self) -> dict:
        d: dict[str, Any] = {
            "summary": self.summary,
            "root_causes": self.root_causes,
            "priority_order": self.priority_order,
            "fix_plan": self.fix_plan,
            "risk_assessment": self.risk_assessment,
            "config_context": self.config_context,
            "actions_taken": self.actions_taken,
            "before_count": self.before_count,
            "after_count": self.after_count,
        }
        if self.fixed_config is not None:
            d["fixed_config"] = json.loads(self.fixed_config)
        if self.residual_findings:
            d["residual_findings"] = [
                {"severity": f.severity.value, "rule": f.rule, "message": f.message}
                for f in self.residual_findings
            ]
        return d


# ---------------------------------------------------------------------------
# System prompt — the agent's identity and reasoning framework
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are SparkGuard Agent, an expert Spark performance engineer.

You are given:
1. A Spark submit JSON config
2. Findings from a deterministic static linter (these are FACTS, not suggestions)
3. Actions the agent already took (auto-fixes applied)
4. Remaining issues that couldn't be auto-fixed

The linter already ran AND applied fixes. Your job is to REASON about the results:

1. **Root Cause Analysis**: Look at the findings together. Are multiple findings symptoms of a single underlying issue? (e.g., AQE disabled + high shuffle partitions + skew settings present all point to someone accidentally disabling AQE)

2. **Workload Inference**: From the config, infer what kind of workload this is. High executor count + large memory = big ETL? Small executors + dynamic allocation = interactive analytics? This context changes what matters.

3. **Priority**: Focus on the REMAINING issues (the ones the auto-fixer couldn't handle). Order by actual impact.

4. **Fix Plan**: For remaining issues, produce a numbered plan. Explain WHY each fix matters for THIS specific config. Don't repeat fixes already applied.

5. **Risk Assessment**: What happens if they ship the FIXED config with the remaining issues? Be specific.

Respond in valid JSON with these fields:
{
  "summary": "2-3 sentence executive summary including what was fixed and what remains",
  "root_causes": ["cause 1", "cause 2"],
  "priority_order": ["rule-id-1", "rule-id-2"],
  "fix_plan": "numbered step-by-step plan for REMAINING issues only",
  "risk_assessment": "what happens if remaining issues are unfixed",
  "config_context": "what you infer about the workload"
}"""


def _build_user_prompt(
    config_json: str,
    report: LintReport,
    code_context: str | None = None,
    actions_taken: list[str] | None = None,
    after_count: int | None = None,
    residual_findings: list[Finding] | None = None,
) -> str:
    """Build the user message with config + findings + action results for the LLM."""
    parts = [
        "## Spark Config\n```json",
        config_json,
        "```\n",
        f"## Lint Findings ({report.error_count} errors, {report.warn_count} warnings, {report.info_count} info)\n",
    ]

    for f in report.findings:
        parts.append(f"- [{f.severity.value}] {f.rule}: {f.message}")

    # Show the LLM what the agent already did — it should reason about residuals
    if actions_taken:
        parts.append(f"\n## Actions Already Taken ({len(actions_taken)} fixes applied)")
        for action in actions_taken:
            parts.append(f"- ✓ {action}")
        parts.append(f"\nAfter fixes: {after_count} finding(s) remain.")
        if residual_findings:
            parts.append("\n## Remaining Issues (need manual intervention)")
            for f in residual_findings:
                parts.append(f"- [{f.severity.value}] {f.rule}: {f.message}")

    if code_context:
        parts.append(f"\n## Code Context\n```\n{code_context}\n```")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# LLM call — uses httpx to avoid heavy SDK dependencies
# ---------------------------------------------------------------------------

def _call_llm(config: AgentConfig, system: str, user: str) -> str:
    """Call an OpenAI-compatible chat completions endpoint.

    Uses httpx (stdlib-friendly) to avoid requiring openai SDK.
    Falls back to urllib if httpx is not available.
    """
    import urllib.request
    import urllib.error

    payload = json.dumps({
        "model": config.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": config.max_tokens,
        "temperature": config.temperature,
        "response_format": {"type": "json_object"},
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{config.base_url.rstrip('/')}/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config.api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM API error {e.code}: {error_body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"LLM API connection error: {e.reason}") from e


def _parse_llm_response(raw: str) -> dict:
    """Parse the LLM's JSON response, handling markdown fencing."""
    text = raw.strip()
    if text.startswith("```"):
        # Strip ```json ... ```
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    return json.loads(text)


# ---------------------------------------------------------------------------
# Core agent loop: Observe → Act → Check
# ---------------------------------------------------------------------------

def _observe_act_check(config_json: str, auto_fix: bool = True) -> dict:
    """Run the deterministic agent loop.

    Returns a dict with all the data needed to build an AgentAnalysis,
    regardless of whether an LLM is used for reasoning.

    IMPORTANT: The "check" step re-lints the fixed config. This is a
    consistency check (no contradictions, math adds up), NOT runtime
    verification. Use `sparkguard verify` for real Spark validation.
    """
    # ── OBSERVE ──────────────────────────────────────────────────────────
    report = lint(config_json)
    before_count = len(report.findings)

    # ── ACT ──────────────────────────────────────────────────────────────
    actions_taken: list[str] = []
    fixed_json: str | None = None
    after_count = before_count
    residual_findings: list[Finding] = list(report.findings)

    if auto_fix and report.findings:
        fixed_json, descriptions = fix_config(config_json)
        actions_taken.extend(descriptions)

        if actions_taken:
            # ── CHECK (static consistency, NOT runtime verification) ──────
            recheck_report = lint(fixed_json)
            after_count = len(recheck_report.findings)
            residual_findings = recheck_report.findings

            # Detect if fix introduced NEW issues (regression guard)
            original_rules = {f.rule for f in report.findings}
            new_rules = {f.rule for f in recheck_report.findings} - original_rules
            if new_rules:
                actions_taken.append(
                    f"⚠ Fix introduced {len(new_rules)} new issue(s): {', '.join(new_rules)}"
                )

    # ── DRY-RUN (optional, real Spark validation) ────────────────────────
    dry_run_result = None
    try:
        from sparkguard.verify import dry_run, find_spark_submit
        if find_spark_submit():
            target = fixed_json if fixed_json else config_json
            dry_run_result = dry_run(target)
            if dry_run_result.accepted:
                actions_taken.append(
                    f"✓ spark-submit dry-run: Spark {dry_run_result.spark_version or ''} accepts this config"
                )
            else:
                for err in dry_run_result.spark_errors:
                    actions_taken.append(f"✗ spark-submit: {err}")
            for key in dry_run_result.deprecated_keys:
                actions_taken.append(f"⚠ Spark deprecated: {key}")
    except Exception:
        pass  # spark-submit not available, skip silently

    return {
        "report": report,
        "before_count": before_count,
        "after_count": after_count,
        "actions_taken": actions_taken,
        "fixed_config": fixed_json if actions_taken else None,
        "residual_findings": residual_findings,
        "dry_run_result": dry_run_result,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze(
    config_json: str,
    code_context: str | None = None,
    agent_config: AgentConfig | None = None,
    auto_fix: bool = True,
) -> AgentAnalysis:
    """Run the full agent loop: observe → act → verify → reason (LLM).

    The agent DOES things, then asks the LLM to explain what happened
    and what's still left. This is the opposite of "paste into ChatGPT."

    Args:
        config_json: Raw Spark submit JSON config.
        code_context: Optional source code snippet for cross-analysis.
        agent_config: LLM configuration. Uses env vars if not provided.
        auto_fix: Whether to apply auto-fixes (default True).

    Returns:
        AgentAnalysis with actions taken, before/after counts, and LLM reasoning.
    """
    if agent_config is None:
        agent_config = AgentConfig.from_env()

    if not agent_config.api_key:
        raise RuntimeError(
            "No API key configured. Set SPARKGUARD_API_KEY or OPENAI_API_KEY "
            "environment variable, or pass an AgentConfig."
        )

    # Steps 1-3: Observe → Act → Check (deterministic)
    loop = _observe_act_check(config_json, auto_fix=auto_fix)
    report: LintReport = loop["report"]

    if not report.findings:
        return AgentAnalysis(
            summary="Config looks clean — no issues detected by static analysis.",
            root_causes=[],
            priority_order=[],
            fix_plan="No fixes needed.",
            risk_assessment="Low risk — config passes all checks.",
            config_context="Unable to infer without findings.",
            raw_findings=[],
        )

    # Step 4: Reason — LLM explains what the agent found AND did
    user_prompt = _build_user_prompt(
        config_json, report, code_context,
        actions_taken=loop["actions_taken"],
        after_count=loop["after_count"],
        residual_findings=loop["residual_findings"],
    )
    raw_response = _call_llm(agent_config, SYSTEM_PROMPT, user_prompt)
    parsed = _parse_llm_response(raw_response)

    return AgentAnalysis(
        summary=parsed.get("summary", ""),
        root_causes=parsed.get("root_causes", []),
        priority_order=parsed.get("priority_order", []),
        fix_plan=parsed.get("fix_plan", ""),
        risk_assessment=parsed.get("risk_assessment", ""),
        config_context=parsed.get("config_context", ""),
        raw_findings=report.findings,
        actions_taken=loop["actions_taken"],
        fixed_config=loop["fixed_config"],
        before_count=loop["before_count"],
        after_count=loop["after_count"],
        residual_findings=loop["residual_findings"],
    )


def analyze_offline(config_json: str, auto_fix: bool = True) -> AgentAnalysis:
    """Run the agent loop WITHOUT an LLM — deterministic observe→act→verify→reason.

    Uses heuristic reasoning instead of an LLM to group findings into root
    causes and build a fix plan. The observe→act→verify loop is identical.
    """
    # Steps 1-3: Observe → Act → Check (same as LLM path)
    loop = _observe_act_check(config_json, auto_fix=auto_fix)
    report: LintReport = loop["report"]

    if not report.findings:
        return AgentAnalysis(
            summary="Config looks clean — no issues detected.",
            root_causes=[],
            priority_order=[],
            fix_plan="No fixes needed.",
            risk_assessment="Low risk.",
            config_context="",
            raw_findings=[],
        )

    # Prioritize: errors first, then warnings, then info
    severity_order = {Severity.ERROR: 0, Severity.WARN: 1, Severity.INFO: 2}
    sorted_findings = sorted(report.findings, key=lambda f: severity_order[f.severity])
    priority_order = [f.rule for f in sorted_findings]

    # Infer root causes by grouping related findings
    root_causes: list[str] = []
    rules = {f.rule for f in report.findings}

    if "duplicate-key" in rules and "aqe-contradictions" in rules:
        root_causes.append(
            "A duplicate JSON key silently overrode a critical setting, "
            "causing a cascade of contradictions (likely a copy-paste error)."
        )
    elif "duplicate-key" in rules:
        root_causes.append("Duplicate JSON keys — last-wins semantics may silently override intended values.")
    if "aqe-contradictions" in rules and "duplicate-key" not in rules:
        root_causes.append("AQE is disabled but its sub-settings are configured — intentional or accidental?")
    if {"low-memory-overhead", "container-memory-sanity"} & rules:
        root_causes.append("Memory configuration is imbalanced — likely under-provisioned overhead or over-sized containers.")
    if {"heartbeat-timeout-mismatch"} & rules:
        root_causes.append("Network timeout configuration will cause healthy executors to be killed during GC pauses.")
    if {"offheap-missing-size"} & rules:
        root_causes.append("Off-heap memory is enabled but not sized — job will fail at startup.")
    if not root_causes:
        root_causes.append("Multiple independent configuration issues detected.")

    # Build fix plan
    fix_steps: list[str] = []
    for i, f in enumerate(sorted_findings, 1):
        fix_steps.append(f"  {i}. [{f.severity.value}] {f.rule}: {f.recommendation}")

    # Risk assessment
    if report.error_count > 0:
        risk = (
            f"{report.error_count} error(s) detected. This config will likely cause "
            "job failures, silent data issues, or massive performance degradation."
        )
    elif report.warn_count > 0:
        risk = (
            f"{report.warn_count} warning(s) detected. Job will run but may waste "
            "resources, run slower than necessary, or fail under edge conditions."
        )
    else:
        risk = "Only informational findings — low risk but room for optimization."

    # Workload inference
    conf_data, _ = parse_spark_config(config_json)
    conf = get_spark_conf(conf_data)
    context_parts: list[str] = []
    exec_mem = conf.get("spark.executor.memory", "")
    exec_instances = conf.get("spark.executor.instances", "")
    if exec_mem:
        context_parts.append(f"executor memory: {exec_mem}")
    if exec_instances:
        context_parts.append(f"executor instances: {exec_instances}")
    if conf.get("spark.dynamicAllocation.enabled"):
        context_parts.append(f"dynamic allocation: {conf['spark.dynamicAllocation.enabled']}")
    config_context = ", ".join(context_parts) if context_parts else "Insufficient config to infer workload type."

    return AgentAnalysis(
        summary=_build_summary(report, loop),
        root_causes=root_causes,
        priority_order=priority_order,
        fix_plan="\n".join(fix_steps),
        risk_assessment=risk,
        config_context=config_context,
        raw_findings=report.findings,
        actions_taken=loop["actions_taken"],
        fixed_config=loop["fixed_config"],
        before_count=loop["before_count"],
        after_count=loop["after_count"],
        residual_findings=loop["residual_findings"],
    )


def _build_summary(report: LintReport, loop: dict) -> str:
    """Build a summary that emphasizes what the agent DID, not just what it found."""
    found = f"Found {report.error_count} error(s), {report.warn_count} warning(s), {report.info_count} info across {len(report.findings)} findings."
    if loop["actions_taken"]:
        fixed_count = loop["before_count"] - loop["after_count"]
        return (
            f"{found} "
            f"Applied {len(loop['actions_taken'])} fix(es), "
            f"resolving {fixed_count} of {loop['before_count']} finding(s)."
        )
    return found

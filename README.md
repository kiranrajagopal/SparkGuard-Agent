# SparkGuard-Agent

**Spark config and code analysis agent that catches misconfigurations and anti-patterns before they cause runtime disasters.**

---

## The Problem

Spark submit JSON configs are deceptively dangerous. JSON allows duplicate keys — Python's `json.loads()` silently keeps the last one. A single copy-paste can override `spark.sql.adaptive.enabled: "true"` with `"false"` buried 40 lines down, and you won't know until your 3-minute job takes 3 hours. Multiply this by the dozens of interdependent Spark settings (memory, shuffle, AQE, dynamic allocation, serialization) and you get a config surface area that's nearly impossible to reason about manually.

SparkGuard is a **Spark config analysis agent**. It starts with static analysis (lint, fix, diff, generate), but goes beyond a CLI tool — it exposes all capabilities as an **MCP server** so AI agents (Copilot, Claude, custom agents) can call it as a tool, and it has its own **agentic reasoning loop** that feeds lint findings to an LLM for root-cause analysis, priority ordering, and fix planning.

**Supports multiple config formats:** JSON, Properties (`spark-defaults.conf`), YAML, HOCON, and embedded configs inside script files (`.ps1`, `.py`, `.sh`) — auto-detected from file extension or content.

---

## What It Does

### Seven CLI commands + real Spark verification

```
sparkguard lint config.json          # Catch problems
sparkguard fix config.json           # Auto-fix safe issues
sparkguard diff old.json new.json    # Compare configs, flag regressions
sparkguard generate                  # Interactive Q&A → optimized config
sparkguard analyze config.json       # Agent analysis: lint + LLM reasoning
sparkguard serve                     # Start MCP server for AI agents
sparkguard verify config.json        # Validate against real Spark (dry-run)
sparkguard verify --compare app1 app2  # Compare actual job metrics
```

### 15 lint rules × 5 config formats

All rules work across JSON, Properties, YAML, HOCON, and script files with embedded configs.
Format is auto-detected from file extension or content sniffing.

| Format | Extension | Duplicate Key Detection | Dependencies |
|--------|-----------|------------------------|--------------|
| JSON | `.json` | ✓ (object_pairs_hook) | None (stdlib) |
| Properties | `.conf`, `.properties` | ✓ | None (stdlib) |
| YAML | `.yaml`, `.yml` | Via PyYAML | `pip install 'sparkguard[yaml]'` |
| HOCON | `.hocon` | Via pyhocon | `pip install 'sparkguard[hocon]'` |
| Scripts | `.ps1`, `.py`, `.sh`, `.scala` | ✓ (extracts embedded JSON) | None (stdlib) |

| Rule | Severity | What It Catches |
|------|----------|-----------------|
| `duplicate-key` | ERROR/WARN | Duplicate JSON keys (last-wins silently overrides) |
| `aqe-contradictions` | ERROR | AQE off but AQE sub-settings configured |
| `high-shuffle-no-aqe` | WARN | ≥2000 shuffle partitions without AQE |
| `storage-fraction-no-cache` | WARN | High storageFraction with no caching |
| `dynamic-alloc-off-high-executors` | WARN | 100+ fixed executors, no dynamic allocation |
| `low-memory-overhead` | WARN | memoryOverhead < max(384m, 10% of executorMemory) |
| `container-memory-sanity` | WARN | Total container memory exceeds 64 GB |
| `missing-spark35-recommendations` | INFO | Missing AQE, Kryo, skew-join settings |
| `aqe-with-low-partitions` | INFO | Very low shuffle partitions limiting AQE |
| `speculation-risk` | WARN | Speculation on without idempotent output |
| `broadcast-threshold-too-high` | ERROR/WARN | Broadcast threshold > 500 MB kills driver |
| `max-partition-bytes` | WARN | Large maxPartitionBytes + small files = stragglers |
| `heartbeat-timeout-mismatch` | ERROR/WARN | Heartbeat ≥ network timeout kills healthy executors |
| `offheap-missing-size` | ERROR | Off-heap enabled without size = startup failure |
| `shuffle-partitions-type` | ERROR/WARN | Float/non-numeric partition count silently ignored |

### Auto-fixer

`sparkguard fix` applies safe, mechanical fixes:
- Removes duplicate keys
- Enables AQE when sub-settings are already configured
- Adds KryoSerializer
- Bumps low memoryOverhead to 10% of executorMemory
- Disables broken off-heap config
- Fixes heartbeat/timeout ratio to 1:3
- Coerces float partition counts to integers

### Config differ

`sparkguard diff` compares two configs and shows:
- Every key added, removed, or changed
- New lint issues introduced (regressions)
- Issues resolved by the change
- Git-aware mode: `sparkguard diff config.json --git HEAD~1`

### Config generator

`sparkguard generate` asks 11 questions about your workload (dataset size, job type, cluster hardware, PySpark vs Scala, join/skew patterns, output format) and produces a tuned config with inline explanations for every setting.

---

## How It Works

```
  any format ──────►┌─────────────────┐
  .json .conf      │  formats.py     │──── auto-detect format
  .yaml .ps1       │  (detect+parse) │     extract from scripts
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │  parser.py      │──── duplicate key findings
                   │  (object_pairs  │     (JSON + Properties)
                   │   _hook)        │
                   └────────┬────────┘
                            │ parsed dict
                   ┌────────▼────────┐
                   │  utils.py       │──── get_spark_conf()
                   │  (10+ nested    │     extracts from any
                   │   config shapes)│     wrapper structure
                   └────────┬────────┘
                            │ flat spark.* conf
                   ┌────────▼────────┐
                   │  engine.py      │──── @rule decorator registry
                   │  run_all_rules()│
                   └────────┬────────┘
                            │ findings[]
                   ┌────────▼────────┐
                   │  rules.py       │──── 15 lint rules
                   │  (each is a fn) │
                   └────────┬────────┘
                            │
      ┌─────────────────────┼─────────────────────┐
      ▼                     ▼                     ▼
 linter.py            fixer.py              diff.py
 (lint)               (fix)                 (diff)
      │                     │                     │
      └──────────┬──────────┴──────────┬──────────┘
                 │                     │
            agent.py             mcp_server.py
       (observe→act→check)   (MCP tools for agents)
                 │                     │
            verify.py                  │
       (dry-run + metrics)             │
                 │                     │
                 └──────────┬──────────┘
                            ▼
                         cli.py
              lint│fix│diff│generate│analyze│verify│serve
```

**Key design decisions:**
- **Duplicate key detection** uses `json.loads(object_pairs_hook=...)` — the only way to see duplicates that Python's JSON parser normally hides.
- **Rule engine** is a simple decorator registry (`@rule("name")`). Adding a new rule is writing one function.
- **Config shape agnostic** — `utils.get_spark_conf()` extracts Spark keys from 10+ wrapper structures:
  - `{"conf": {...}}`, `{"sparkConf": {...}}`, `{"spark_conf": {...}}` (Livy, Databricks)
  - Flat `{"spark.foo": ...}` (top-level keys)
  - Deep nesting up to 6 levels: `{"spec": {"sparkConf": {...}}}` (K8s operator), `{"dag": {"task": {"conf": {...}}}}` (Airflow)
  - Nested YAML/HOCON: `{"spark": {"executor": {"memory": "8g"}}}` → flattened to `spark.executor.memory`
  - EMR/Dataproc key-value lists: `[{"Key": "spark.foo", "Value": "bar"}]`
- **Script extraction** — Spark configs embedded in `.ps1` here-strings, Python triple-quotes, shell heredocs, or `curl` payloads are automatically extracted via brace-matching.
- **Fixer is separate from linter** — lint tells you what's wrong, fix applies safe changes. Never combined implicitly.

---

## Why This Exists

Built out of pain. Working on Xandr adtech Spark pipelines, a duplicate `spark.sql.adaptive.enabled` key in a JSON config silently disabled AQE, turning a 3-minute job into a 3-hour one. There was no tool that catches this — JSON linters don't understand Spark semantics, and Spark itself doesn't warn about contradictory configs.

SparkGuard sits between your editor and `spark-submit` and catches:
1. **Silent config bugs** (duplicate keys, type mismatches)
2. **Contradictions** (AQE off but AQE sub-settings tuned)
3. **Resource math errors** (overhead too low → OOM kills, container too big → won't schedule)
4. **Missing best practices** (no Kryo, no skew join handling)
5. **Regressions** (diff catches when a PR introduces a new problem)

---

## Where It Runs

- **Local CLI** — run before `spark-submit` as a sanity check
- **CI/CD** — `sparkguard lint --severity ERROR --json` returns exit code 1 on errors, perfect for PR gates
- **Pre-commit hook** — block merges that break configs
- **MCP server** — `sparkguard serve` exposes lint/fix/diff/generate as tools for GitHub Copilot, Claude, or any MCP-compatible agent
- **Agent mode** — `sparkguard analyze` feeds findings to an LLM for root-cause analysis and prioritized fix plans

---

## Installation

### From source (current)

```bash
# Clone and install
git clone <repo-url>
cd SparkGuard-Agent

# Basic install (lint, fix, diff, generate, analyze --offline)
pip install -e .

# With MCP server support
pip install -e ".[mcp]"

# With dev/test dependencies
pip install -e ".[dev]"

# With Layer 2 dependencies (future SQL analysis)
pip install -e ".[layer2]"
```

### Dependencies

**Runtime — zero external dependencies.** Layer 1 uses only Python stdlib (`json`, `argparse`, `dataclasses`, `urllib`).

| Package | Required? | What For |
|---------|-----------|----------|
| Python ≥ 3.10 | Yes | Runtime |
| *(none)* | — | Layer 1 has zero pip dependencies |
| `mcp ≥ 1.0` | Optional | MCP server (`sparkguard serve`) |
| `pyyaml ≥ 6.0` | Optional | YAML config files (`sparkguard[yaml]`) |
| `pyhocon ≥ 0.3.60` | Optional | HOCON config files (`sparkguard[hocon]`) |
| `sqlglot ≥ 23.0` | Optional | Layer 2 SQL analysis (future) |
| `pytest ≥ 7.0` | Dev only | Test suite |
| `pytest-cov` | Dev only | Coverage reports |

Agent mode (`sparkguard analyze`) uses any OpenAI-compatible API via `urllib` — no SDK needed. Set `SPARKGUARD_API_KEY` or `OPENAI_API_KEY` environment variable.

---

## Usage Examples

### Lint a config

```bash
# JSON
$ sparkguard lint config.json

# Properties (spark-defaults.conf)
$ sparkguard lint spark-defaults.conf

# YAML
$ sparkguard lint config.yaml

# Script files with embedded Spark config
$ sparkguard lint submit_job.ps1
$ sparkguard lint spark_etl.py

# Format auto-detected from extension or content
$ cat config | sparkguard lint

SparkGuard: config.json
  2 error(s), 3 warning(s), 1 info

[ERROR] Duplicate key 'spark.sql.adaptive.enabled' in JSON object at <root>.
        Value changed from 'true' to 'false' — last-wins semantics silently override.
  Keys: spark.sql.adaptive.enabled
  Fix:  Remove the duplicate entry. Keep only the intended value.
  Rule: duplicate-key
```

### Auto-fix

```bash
$ sparkguard fix bad_config.json -o fixed.json

Applied 4 fix(es):
  ✓ Removed duplicate key 'spark.sql.adaptive.enabled' (kept last value)
  ✓ Enabled AQE to match existing sub-settings
  ✓ Set spark.executor.memoryOverhead to 1638m (10% of executorMemory)
  ✓ Added recommended setting spark.serializer=KryoSerializer

✓ Fixed config written to fixed.json
```

### Diff two configs

```bash
$ sparkguard diff prod.json experiment.json

━━━ SparkGuard Diff: prod.json → experiment.json ━━━

3 config change(s):
  ~ spark.executor.memory: '8g' → '16g'
  + spark.memory.offHeap.enabled: 'true'
  - spark.sql.adaptive.skewJoin.enabled: 'true'

⚠ 1 NEW issue(s) introduced:
[ERROR] Off-heap memory is enabled but spark.memory.offHeap.size is not set.

✓ 0 issue(s) resolved.
```

### Git-aware diff

```bash
$ sparkguard diff config.json --git HEAD~1
# Compares your working copy against the previous commit
```

### Generate a config interactively

```bash
$ sparkguard generate --lint -o my_config.json
# Asks about dataset size, job type, cluster, etc.
# Produces a tuned config with explanations
```

### JSON output for CI

```bash
$ sparkguard lint config.json --json --severity ERROR
# Returns structured JSON, exit code 1 if errors found
```

### Pipe from stdin

```bash
$ cat config.json | sparkguard lint
$ sparkguard fix < broken.json > fixed.json
```

### Agent analysis (LLM-powered)

```bash
# Full agent mode — sends findings to an LLM for root-cause analysis
$ export SPARKGUARD_API_KEY=sk-...
$ sparkguard analyze config.json

━━━ SparkGuard Agent Analysis ━━━

Summary: A duplicate key silently disabled AQE, causing a cascade of
         contradictions across 3 related settings.

Root Causes:
  1. Duplicate 'spark.sql.adaptive.enabled' key overrode true→false (copy-paste error)
  2. Memory overhead at 256m is dangerously low for 16g executors

Fix Plan (priority order):
  1. Remove the duplicate key, keep spark.sql.adaptive.enabled=true
  2. Bump memoryOverhead to at least 1638m (10% of 16g)
  ...

# Offline mode — no API key needed, heuristic analysis
$ sparkguard analyze --offline config.json

# JSON output for programmatic use
$ sparkguard analyze --offline --json config.json

# Use a specific model
$ sparkguard analyze --model gpt-4o config.json
```

### MCP server for AI agents

```bash
# Start the MCP server (requires pip install 'sparkguard[mcp]')
$ sparkguard serve

# Add to your MCP client config (e.g., Claude Desktop, Copilot):
{
  "mcpServers": {
    "sparkguard": {
      "command": "sparkguard",
      "args": ["serve"]
    }
  }
}
```

The MCP server exposes 4 tools:
- `sparkguard_lint` — Lint a config, get structured findings
- `sparkguard_fix` — Auto-fix safe issues, get changed config + descriptions
- `sparkguard_diff` — Compare two configs, detect regressions
- `sparkguard_generate` — Generate optimized config from workload profile

### Real Spark verification

Static analysis catches contradictions and math errors. But the only way to know if Spark actually accepts your config is to **ask Spark**.

#### Level 1: spark-submit dry-run

Runs `spark-submit --verbose` in local mode with your config flags. Spark's own parser validates every key — catches deprecated settings, invalid class names, and version-specific configs that static rules don't know about.

```bash
# Does Spark accept this config?
$ sparkguard verify config.json
✓ Spark 3.5.1 accepts this config.

# When it doesn't:
$ sparkguard verify broken.json
✗ Spark rejected this config (1 error(s)):
  ERROR: IllegalArgumentException: Invalid value for spark.executor.memory
⚠ 1 deprecated key(s):
  - spark.shuffle.consolidateFiles is deprecated

# JSON for CI pipelines
$ sparkguard verify config.json --json
```

Requires `spark-submit` on PATH or `SPARK_HOME` set. Works with any Spark version — the validation is Spark doing it, not us.

#### Level 2: Job metrics comparison

Compare two real job runs via Spark History Server to measure if your config change actually improved things.

```bash
# Did the config change make the job faster?
$ sparkguard verify --compare application_123_0001 application_123_0002

━━━ SparkGuard Metrics Comparison ━━━

Duration:  3.5m → 1.2m  (-65.7%) ✓
Shuffle:   2.3GB → 890.0MB  (-61.3%) ✓
GC time:   45000ms → 12000ms
Failed:    5 → 0
Killed:    2 → 0

# Custom History Server URL
$ sparkguard verify --compare app1 app2 --history-server http://spark-history:18080
```

Requires a running Spark History Server (default: `http://localhost:18080` or `SPARK_HISTORY_SERVER` env var).

#### What each level proves

| Level | What it proves | What it doesn't prove |
|-------|---------------|----------------------|
| **Static lint** (`sparkguard lint`) | No contradictions, math checks out, best practices followed | Spark might reject keys your version doesn't support |
| **Dry-run** (`sparkguard verify`) | Spark's own parser accepts every key/value | Job will actually run faster |
| **Metrics** (`sparkguard verify --compare`) | Job duration, shuffle, GC, failures changed | Causation (other factors may differ between runs) |

The agent's `analyze` command automatically runs a dry-run when `spark-submit` is available, and reports the result alongside static findings.

---

## Project Structure

```
SparkGuard-Agent/
├── pyproject.toml              # Package definition, CLI entry point
├── sparkguard/
│   ├── __init__.py
│   ├── models.py               # Finding, LintReport, Severity
│   ├── formats.py              # Multi-format parser (JSON, Properties, YAML, HOCON, scripts)
│   ├── parser.py               # Duplicate-key-aware JSON parser
│   ├── utils.py                # Config extraction, memory parsing, bool coercion
│   ├── engine.py               # Rule registry (@rule decorator) + runner
│   ├── rules.py                # 15 lint rules
│   ├── linter.py               # Orchestrator: parser → engine → report
│   ├── fixer.py                # Auto-fix engine with @fixer decorator
│   ├── diff.py                 # Config differ with regression detection
│   ├── generator.py            # Interactive config generator
│   ├── agent.py                # Agentic LLM reasoning (observe → act → check)
│   ├── verify.py               # Real Spark validation (dry-run + History Server)
│   ├── mcp_server.py           # MCP server exposing tools for AI agents
│   └── cli.py                  # CLI: lint, fix, diff, generate, analyze, verify, serve
├── tests/
│   ├── test_parser.py          # Duplicate key detection
│   ├── test_rules.py           # Original 10 rules
│   ├── test_new_rules.py       # 5 new adtech rules
│   ├── test_diff.py            # Diff + regression detection
│   ├── test_fixer.py           # Auto-fix correctness + idempotency
│   ├── test_generator.py       # Config generation + linter self-check
│   ├── test_integration.py     # "The 3-hour bug" end-to-end test
│   ├── test_agent.py           # Agent reasoning (offline + mocked LLM)
│   ├── test_mcp_server.py      # MCP tool definitions + handlers
│   ├── test_verify.py          # Dry-run + History Server (mocked subprocess)
│   └── test_formats.py         # Multi-format + script extraction + cross-format diff
└── examples/
    ├── bad_config.json         # Kitchen-sink bad config (JSON)
    ├── bad_config.conf         # Same config in properties format
    ├── bad_config.yaml         # Same config in YAML format
    └── submit_job.ps1          # PowerShell Livy submit with embedded config
```

---

## Running Tests

```bash
pip install -e ".[dev]"
python -m pytest -v            # 146 tests, runs in <0.3s
python -m pytest --cov=sparkguard --cov-report=term-missing
```

---

## Roadmap

- **Layer 2 — Code × Config analysis**: Use sqlglot to parse SQL, detect anti-patterns (shuffle explosions, missing broadcast hints, COUNT DISTINCT on wide rows), cross-reference against config settings
- **`.sparkguardrc`**: Per-project rule customization (suppress rules, change severity)
- **VS Code extension**: Inline warnings in config files
- **Databricks / EMR native integration**: Direct API calls to validate configs against running clusters

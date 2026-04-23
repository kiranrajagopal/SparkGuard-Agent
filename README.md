# SparkGuard-Agent

**Spark config and code analysis agent that catches misconfigurations and anti-patterns before they cause runtime disasters.**

---

## The Problem

Spark submit JSON configs are deceptively dangerous. JSON allows duplicate keys — Python's `json.loads()` silently keeps the last one. A single copy-paste can override `spark.sql.adaptive.enabled: "true"` with `"false"` buried 40 lines down, and you won't know until your 3-minute job takes 3 hours. Multiply this by the dozens of interdependent Spark settings (memory, shuffle, AQE, dynamic allocation, serialization) and you get a config surface area that's nearly impossible to reason about manually.

SparkGuard is a static analysis tool for Spark configs. It parses your submit JSON, catches duplicate keys, flags contradictions, validates resource math, and can auto-fix the safe issues — all before the job hits the cluster.

---

## What It Does

### Four CLI commands

```
sparkguard lint config.json          # Catch problems
sparkguard fix config.json           # Auto-fix safe issues
sparkguard diff old.json new.json    # Compare configs, flag regressions
sparkguard generate                  # Interactive Q&A → optimized config
```

### 15 lint rules

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
                    ┌─────────────────┐
  JSON input ──────►│  parser.py      │──── duplicate key findings
                    │  (object_pairs  │
                    │   _hook)        │
                    └────────┬────────┘
                             │ parsed dict
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
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         linter.py      fixer.py       diff.py
         (lint)         (fix)          (diff)
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                          cli.py
```

**Key design decisions:**
- **Duplicate key detection** uses `json.loads(object_pairs_hook=...)` — the only way to see duplicates that Python's JSON parser normally hides.
- **Rule engine** is a simple decorator registry (`@rule("name")`). Adding a new rule is writing one function.
- **Config shape agnostic** — `utils.get_spark_conf()` handles `{"conf": {...}}`, `{"sparkConf": {...}}`, flat `{"spark.foo": ...}`, and one-level nested configs.
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
- **Designed for extension** — structured to later wrap as an MCP server or VS Code extension (Layer 2 with sqlglot for code analysis is planned)

---

## Installation

### From source (current)

```bash
# Clone and install
git clone <repo-url>
cd SparkGuard-Agent

# Basic install
pip install -e .

# With dev/test dependencies
pip install -e ".[dev]"

# With Layer 2 dependencies (future SQL analysis)
pip install -e ".[layer2]"
```

### Dependencies

**Runtime — zero external dependencies.** Layer 1 uses only Python stdlib (`json`, `argparse`, `dataclasses`).

| Package | Required? | What For |
|---------|-----------|----------|
| Python ≥ 3.10 | Yes | Runtime |
| *(none)* | — | Layer 1 has zero pip dependencies |
| `sqlglot ≥ 23.0` | Optional | Layer 2 SQL analysis (future) |
| `pytest ≥ 7.0` | Dev only | Test suite |
| `pytest-cov` | Dev only | Coverage reports |

---

## Usage Examples

### Lint a config

```bash
$ sparkguard lint config.json

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

---

## Project Structure

```
SparkGuard-Agent/
├── pyproject.toml              # Package definition, CLI entry point
├── sparkguard/
│   ├── __init__.py
│   ├── models.py               # Finding, LintReport, Severity
│   ├── parser.py               # Duplicate-key-aware JSON parser
│   ├── utils.py                # Config extraction, memory parsing, bool coercion
│   ├── engine.py               # Rule registry (@rule decorator) + runner
│   ├── rules.py                # 15 lint rules
│   ├── linter.py               # Orchestrator: parser → engine → report
│   ├── fixer.py                # Auto-fix engine with @fixer decorator
│   ├── diff.py                 # Config differ with regression detection
│   ├── generator.py            # Interactive config generator
│   └── cli.py                  # CLI: lint, fix, diff, generate subcommands
├── tests/
│   ├── test_parser.py          # Duplicate key detection
│   ├── test_rules.py           # Original 10 rules
│   ├── test_new_rules.py       # 5 new adtech rules
│   ├── test_diff.py            # Diff + regression detection
│   ├── test_fixer.py           # Auto-fix correctness + idempotency
│   ├── test_generator.py       # Config generation + linter self-check
│   └── test_integration.py     # "The 3-hour bug" end-to-end test
└── examples/
    └── bad_config.json         # Kitchen-sink bad config for demo
```

---

## Running Tests

```bash
pip install -e ".[dev]"
python -m pytest -v            # 68 tests, runs in <0.2s
python -m pytest --cov=sparkguard --cov-report=term-missing
```

---

## Roadmap

- **Layer 2 — Code × Config analysis**: Use sqlglot to parse SQL, detect anti-patterns (shuffle explosions, missing broadcast hints, COUNT DISTINCT on wide rows), cross-reference against config settings
- **`.sparkguardrc`**: Per-project rule customization (suppress rules, change severity)
- **YAML/HOCON support**: Many teams use Typesafe Config, not JSON
- **MCP server wrapper**: Expose as a tool for AI agents
- **VS Code extension**: Inline warnings in config files

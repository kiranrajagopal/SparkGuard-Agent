"""SparkGuard CLI entry point."""

from __future__ import annotations

import argparse
import json
import sys

from sparkguard.linter import lint
from sparkguard.models import Severity


def _run_lint(args: argparse.Namespace) -> int:
    """Execute the lint subcommand."""
    if args.config_file:
        try:
            with open(args.config_file, "r", encoding="utf-8") as f:
                raw = f.read()
        except FileNotFoundError:
            print(f"Error: file not found: {args.config_file}", file=sys.stderr)
            return 2
        except OSError as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            return 2
        config_path = args.config_file
    else:
        if sys.stdin.isatty():
            print("Reading from stdin (Ctrl+D / Ctrl+Z to end)...", file=sys.stderr)
        raw = sys.stdin.read()
        config_path = "<stdin>"

    try:
        report = lint(raw, config_path=config_path)
    except (json.JSONDecodeError, ValueError, ImportError) as e:
        print(f"Error: cannot parse config — {e}", file=sys.stderr)
        return 2

    severity_order = {Severity.ERROR: 0, Severity.WARN: 1, Severity.INFO: 2}
    min_sev = Severity[args.severity]
    min_ord = severity_order[min_sev]
    report.findings = [f for f in report.findings if severity_order[f.severity] <= min_ord]

    if args.json_output:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        if not report.findings:
            print(f"✓ {config_path}: No issues found.")
        else:
            print(f"SparkGuard: {config_path}")
            print(f"  {report.error_count} error(s), {report.warn_count} warning(s), {report.info_count} info\n")
            for finding in report.findings:
                print(finding.format_text())
                print()

    return 1 if report.error_count > 0 else 0


def _run_generate(args: argparse.Namespace) -> int:
    """Execute the generate subcommand."""
    from sparkguard.generator import (
        collect_profile_interactive,
        format_config_json,
        format_config_with_explanations,
        generate_config,
    )

    profile = collect_profile_interactive()
    conf = generate_config(profile)

    print()
    print(format_config_with_explanations(conf, profile))

    # Optionally lint the generated config
    if args.lint_output:
        print("\n━━━ Linting generated config ━━━")
        raw_json = format_config_json(conf)
        report = lint(raw_json, config_path="<generated>")
        if not report.findings:
            print("✓ Generated config passes all checks.")
        else:
            print(f"  {report.error_count} error(s), {report.warn_count} warning(s), {report.info_count} info\n")
            for finding in report.findings:
                print(finding.format_text())
                print()

    # Optionally write to file
    if args.output:
        raw_json = format_config_json(conf)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(raw_json + "\n")
        print(f"\n✓ Config written to {args.output}")

    return 0


def _run_diff(args: argparse.Namespace) -> int:
    """Execute the diff subcommand."""
    import subprocess

    from sparkguard.diff import diff_configs, format_diff_text

    if args.git:
        # Git-aware: compare current file against a git revision
        if not args.config_file:
            print("Error: --git requires a config file path.", file=sys.stderr)
            return 2
        rev = args.git
        try:
            base_raw = subprocess.check_output(
                ["git", "show", f"{rev}:{args.config_file}"],
                text=True,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError:
            print("Error: git is not installed or not in PATH.", file=sys.stderr)
            return 2
        except subprocess.CalledProcessError as e:
            print(
                f"Error: could not get '{args.config_file}' from git revision '{rev}'. "
                f"{e.stderr.strip()}",
                file=sys.stderr,
            )
            return 2
        try:
            with open(args.config_file, "r", encoding="utf-8") as f:
                target_raw = f.read()
        except (FileNotFoundError, OSError) as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            return 2
        base_label = f"{args.config_file} @ {rev}"
        target_label = f"{args.config_file} (working copy)"
    else:
        # Two-file diff
        if not args.config_file or not args.target_file:
            print("Error: diff requires two config files (or use --git).", file=sys.stderr)
            return 2
        try:
            with open(args.config_file, "r", encoding="utf-8") as f:
                base_raw = f.read()
            with open(args.target_file, "r", encoding="utf-8") as f:
                target_raw = f.read()
        except (FileNotFoundError, OSError) as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            return 2
        base_label = args.config_file
        target_label = args.target_file

    try:
        report = diff_configs(base_raw, target_raw, base_label, target_label)
    except (json.JSONDecodeError, ValueError, ImportError) as e:
        print(f"Error: cannot parse config — {e}", file=sys.stderr)
        return 2

    if args.json_output:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        print(format_diff_text(report))

    return 1 if report.has_regressions else 0


def _run_fix(args: argparse.Namespace) -> int:
    """Execute the fix subcommand."""
    from sparkguard.fixer import fix_config

    if args.config_file:
        try:
            with open(args.config_file, "r", encoding="utf-8") as f:
                raw = f.read()
        except (FileNotFoundError, OSError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2
    else:
        if sys.stdin.isatty():
            print("Reading from stdin (Ctrl+D / Ctrl+Z to end)...", file=sys.stderr)
        raw = sys.stdin.read()

    try:
        fixed_output, descriptions = fix_config(raw, filename=args.config_file or "")
    except (json.JSONDecodeError, ValueError, ImportError) as e:
        print(f"Error: cannot parse config — {e}", file=sys.stderr)
        return 2

    if not descriptions:
        print("✓ No auto-fixable issues found.", file=sys.stderr)
        print(raw)
        return 0

    # Show what was fixed
    print(f"Applied {len(descriptions)} fix(es):", file=sys.stderr)
    for desc in descriptions:
        print(f"  ✓ {desc}", file=sys.stderr)
    print(file=sys.stderr)

    # Output or write
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(fixed_output + "\n")
        print(f"✓ Fixed config written to {args.output}", file=sys.stderr)
    elif args.in_place and args.config_file:
        with open(args.config_file, "w", encoding="utf-8") as f:
            f.write(fixed_output + "\n")
        print(f"✓ Fixed config written back to {args.config_file}", file=sys.stderr)
    else:
        print(fixed_output)

    return 0


def _run_analyze(args: argparse.Namespace) -> int:
    """Execute the analyze subcommand — agentic observe→act→verify→reason loop."""
    from sparkguard.agent import analyze, analyze_offline, AgentConfig

    if args.config_file:
        try:
            with open(args.config_file, "r", encoding="utf-8") as f:
                raw = f.read()
        except FileNotFoundError:
            print(f"Error: file not found: {args.config_file}", file=sys.stderr)
            return 2
        except OSError as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            return 2
    else:
        if sys.stdin.isatty():
            print("Reading from stdin (Ctrl+D / Ctrl+Z to end)...", file=sys.stderr)
        raw = sys.stdin.read()

    auto_fix = not args.no_fix

    try:
        if args.offline:
            result = analyze_offline(raw, auto_fix=auto_fix)
        else:
            config = AgentConfig.from_env()
            if args.model:
                config.model = args.model
            try:
                result = analyze(raw, agent_config=config, auto_fix=auto_fix)
            except RuntimeError as e:
                if "No API key" in str(e):
                    print(
                        "No API key found. Falling back to offline analysis.\n"
                        "Set SPARKGUARD_API_KEY or OPENAI_API_KEY for full agent mode.\n",
                        file=sys.stderr,
                    )
                    result = analyze_offline(raw, auto_fix=auto_fix)
                else:
                    print(f"Error: {e}", file=sys.stderr)
                    return 1

        if args.json_output:
            print(json.dumps(result.as_dict(), indent=2))
        else:
            print(result.format_text())

        # Write fixed config if requested and fixes were applied
        if result.fixed_config:
            if args.output:
                with open(args.output, "w", encoding="utf-8") as f:
                    f.write(result.fixed_config + "\n")
                print(f"✓ Fixed config written to {args.output}", file=sys.stderr)
            elif args.in_place and args.config_file:
                with open(args.config_file, "w", encoding="utf-8") as f:
                    f.write(result.fixed_config + "\n")
                print(f"✓ Fixed config written back to {args.config_file}", file=sys.stderr)

    except (json.JSONDecodeError, ValueError, ImportError) as e:
        print(f"Error: cannot parse config — {e}", file=sys.stderr)
        return 2

    return 0


def _run_serve(_args: argparse.Namespace) -> int:
    """Start the MCP server."""
    import asyncio
    from sparkguard.mcp_server import run_mcp_server, HAS_MCP

    if not HAS_MCP:
        print(
            "Error: MCP SDK not installed. Install with:\n"
            "  pip install 'sparkguard[mcp]'\n",
            file=sys.stderr,
        )
        return 1

    print("Starting SparkGuard MCP server (stdio)...", file=sys.stderr)
    asyncio.run(run_mcp_server())
    return 0


def _run_verify(args: argparse.Namespace) -> int:
    """Verify a config against real Spark infrastructure."""
    from sparkguard.verify import dry_run, compare_jobs, find_spark_submit

    if args.compare:
        # Compare two job runs via History Server
        app_ids = args.compare
        if len(app_ids) != 2:
            print("Error: --compare requires exactly 2 app IDs (before after)", file=sys.stderr)
            return 2
        try:
            comparison = compare_jobs(
                app_ids[0], app_ids[1],
                history_server_url=args.history_server,
            )
            if args.json_output:
                print(json.dumps(comparison.as_dict(), indent=2))
            else:
                print(comparison.format_text())
            return 0
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    # Dry-run mode (default)
    if not args.config_file:
        print("Error: config file required for dry-run.", file=sys.stderr)
        return 2

    try:
        with open(args.config_file, "r", encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        print(f"Error: file not found: {args.config_file}", file=sys.stderr)
        return 2
    except OSError as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return 2

    spark_path = find_spark_submit()
    if not spark_path:
        print(
            "Error: spark-submit not found.\n"
            "  Set SPARK_HOME or add spark-submit to PATH.\n"
            "  This validates your config against a real Spark installation.\n",
            file=sys.stderr,
        )
        return 1

    result = dry_run(raw, spark_path)
    if args.json_output:
        print(json.dumps(result.as_dict(), indent=2))
    else:
        print(result.format_text())

    return 0 if result.accepted else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sparkguard",
        description="Spark config and code analysis agent — catches misconfigurations and anti-patterns.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- lint subcommand ---
    lint_parser = subparsers.add_parser("lint", help="Lint a Spark config file.")
    lint_parser.add_argument(
        "config_file",
        nargs="?",
        help="Path to Spark submit JSON config. Reads stdin if omitted.",
    )
    lint_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Output findings as JSON.",
    )
    lint_parser.add_argument(
        "--severity",
        choices=["ERROR", "WARN", "INFO"],
        default="INFO",
        help="Minimum severity to display (default: INFO).",
    )

    # --- generate subcommand ---
    gen_parser = subparsers.add_parser(
        "generate", aliases=["gen"],
        help="Interactively generate an optimized Spark config.",
    )
    gen_parser.add_argument(
        "-o", "--output",
        help="Write generated config JSON to this file.",
    )
    gen_parser.add_argument(
        "--lint",
        dest="lint_output",
        action="store_true",
        help="Lint the generated config after generation.",
    )

    # --- diff subcommand ---
    diff_parser = subparsers.add_parser(
        "diff",
        help="Compare two Spark configs and flag regressions.",
    )
    diff_parser.add_argument(
        "config_file",
        help="Base config file (the 'before').",
    )
    diff_parser.add_argument(
        "target_file",
        nargs="?",
        help="Target config file (the 'after'). Not needed with --git.",
    )
    diff_parser.add_argument(
        "--git",
        metavar="REV",
        help="Compare working copy against a git revision (e.g., HEAD, HEAD~1, main).",
    )
    diff_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Output diff as JSON.",
    )

    # --- fix subcommand ---
    fix_parser = subparsers.add_parser(
        "fix",
        help="Auto-fix safe issues in a Spark config.",
    )
    fix_parser.add_argument(
        "config_file",
        nargs="?",
        help="Config file to fix. Reads stdin if omitted.",
    )
    fix_parser.add_argument(
        "-o", "--output",
        help="Write fixed config to this file (default: stdout).",
    )
    fix_parser.add_argument(
        "-i", "--in-place",
        action="store_true",
        help="Fix the file in place (overwrites the input file).",
    )

    # --- analyze subcommand ---
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Agent-powered analysis: observe → auto-fix → verify → reason.",
    )
    analyze_parser.add_argument(
        "config_file",
        nargs="?",
        help="Config file to analyze. Reads stdin if omitted.",
    )
    analyze_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Output analysis as JSON.",
    )
    analyze_parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip LLM — use rule-based heuristic reasoning only.",
    )
    analyze_parser.add_argument(
        "--model",
        help="Override LLM model (default: gpt-4o-mini or SPARKGUARD_MODEL env).",
    )
    analyze_parser.add_argument(
        "--no-fix",
        action="store_true",
        help="Don't auto-fix — only observe and reason (like a linter with brains).",
    )
    analyze_parser.add_argument(
        "-o", "--output",
        help="Write the fixed config to this file.",
    )
    analyze_parser.add_argument(
        "-i", "--in-place",
        action="store_true",
        help="Write the fixed config back to the input file.",
    )

    # --- serve subcommand ---
    subparsers.add_parser(
        "serve",
        help="Start the SparkGuard MCP server (stdio transport).",
    )

    # --- verify subcommand ---
    verify_parser = subparsers.add_parser(
        "verify",
        help="Validate config against real Spark (dry-run) or compare job metrics.",
    )
    verify_parser.add_argument(
        "config_file",
        nargs="?",
        help="Config file to validate via spark-submit dry-run.",
    )
    verify_parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("BEFORE_APP_ID", "AFTER_APP_ID"),
        help="Compare two Spark job runs via History Server.",
    )
    verify_parser.add_argument(
        "--history-server",
        help="Spark History Server URL (default: SPARK_HISTORY_SERVER env or localhost:18080).",
    )
    verify_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Output as JSON.",
    )

    args = parser.parse_args(argv)

    if args.command in ("generate", "gen"):
        return _run_generate(args)
    elif args.command == "lint":
        return _run_lint(args)
    elif args.command == "diff":
        return _run_diff(args)
    elif args.command == "fix":
        return _run_fix(args)
    elif args.command == "analyze":
        return _run_analyze(args)
    elif args.command == "serve":
        return _run_serve(args)
    elif args.command == "verify":
        return _run_verify(args)
    else:
        # No subcommand — show help
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())

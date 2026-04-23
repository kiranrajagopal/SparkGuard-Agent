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
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON — {e}", file=sys.stderr)
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
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON — {e}", file=sys.stderr)
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
        fixed_json, descriptions = fix_config(raw)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON — {e}", file=sys.stderr)
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
            f.write(fixed_json + "\n")
        print(f"✓ Fixed config written to {args.output}", file=sys.stderr)
    elif args.in_place and args.config_file:
        with open(args.config_file, "w", encoding="utf-8") as f:
            f.write(fixed_json + "\n")
        print(f"✓ Fixed config written back to {args.config_file}", file=sys.stderr)
    else:
        print(fixed_json)

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sparkguard",
        description="Lint Spark submit JSON configs for misconfigurations and anti-patterns.",
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

    args = parser.parse_args(argv)

    if args.command in ("generate", "gen"):
        return _run_generate(args)
    elif args.command == "lint":
        return _run_lint(args)
    elif args.command == "diff":
        return _run_diff(args)
    elif args.command == "fix":
        return _run_fix(args)
    else:
        # No subcommand — show help
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())

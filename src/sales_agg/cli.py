"""Command line entry point: `sales-agg <command>`."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .doctor import run_doctor
from .i18n import resolve_lang, t

DEFAULT_ROWS = 100_000
DEFAULT_SEED = 42
DEFAULT_BENCH_ROWS = "100000,1000000,10000000"
STEPS = ["generate", "explore", "aggregate", "window", "explain", "save"]
COMMANDS = ["doctor", *STEPS, "timezone", "bench", "tour"]
WORKDIR_ENV = "SALES_AGG_WORKDIR"


def _scan_lang(argv: Sequence[str]) -> str | None:
    """Find --lang in argv before the full parser exists (needed to translate --help)."""
    for i, arg in enumerate(argv):
        if arg == "--lang" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--lang="):
            return arg.split("=", 1)[1]
    return None


def build_parser(lang: str) -> argparse.ArgumentParser:
    """Build the argument parser with help text in the chosen language."""
    parser = argparse.ArgumentParser(
        prog="sales-agg",
        description=t(lang, "app.description"),
        epilog=t(lang, "app.epilog"),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--lang", choices=["en", "ko"], default=None, help=t(lang, "help.lang"))
    sub = parser.add_subparsers(dest="command", metavar="<command>", required=True)
    for name in COMMANDS:
        text = t(lang, f"help.cmd.{name}")
        p = sub.add_parser(name, help=text, description=text)
        p.add_argument("--lang", choices=["en", "ko"], default=argparse.SUPPRESS, help=t(lang, "help.lang"))
        p.add_argument("--workdir", type=Path, default=None, help=t(lang, "help.workdir"))
        if name == "doctor":
            continue
        p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=t(lang, "help.seed", default=DEFAULT_SEED))
        p.add_argument("--ui", action="store_true", help=t(lang, "help.ui"))
        if name == "bench":
            p.add_argument("--rows", default=DEFAULT_BENCH_ROWS,
                           help=t(lang, "help.bench_rows", default=DEFAULT_BENCH_ROWS))
            p.add_argument("--repeat", type=int, default=1, help=t(lang, "help.repeat", default=1))
        else:
            p.add_argument("--rows", type=int, default=DEFAULT_ROWS,
                           help=t(lang, "help.rows", default=DEFAULT_ROWS))
    return parser


def _wait_for_ui(lang: str) -> None:
    print("\n" + t(lang, "ui.open"))
    try:
        input()
    except EOFError:
        pass


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI; returns the exit code (0 ok, 2 setup or input problem)."""
    args_list = list(sys.argv[1:] if argv is None else argv)
    try:
        lang = resolve_lang(_scan_lang(args_list), os.environ)
    except ValueError as err:
        print(f"sales-agg: error: {err}", file=sys.stderr)
        return 2
    args = build_parser(lang).parse_args(args_list)
    workdir = (args.workdir or Path(os.environ.get(WORKDIR_ENV, "."))).resolve()

    if args.command == "doctor":
        return run_doctor(lang, workdir)

    # heavy imports only when Spark is needed, so `doctor` and `--help` stay fast
    from .session import SessionError, build_session
    from .steps import Context, aggregate, bench, explain, explore, generate, save, timezone, window

    if args.command == "bench":
        try:
            sizes = bench.parse_sizes(args.rows)
            if args.repeat < 1:
                raise ValueError("--repeat must be at least 1")
            bench.run(workdir, sizes, args.repeat, args.seed, lang)
        except (ValueError, SessionError) as err:
            print(f"sales-agg: error: {err}", file=sys.stderr)
            return 2
        return 0

    if args.rows < 1:
        print(f"sales-agg: error: {t(lang, 'error.rows')}", file=sys.stderr)
        return 2
    try:
        spark = build_session(workdir, ui=args.ui, lang=lang)
    except SessionError as err:
        print(f"sales-agg: error: {err}", file=sys.stderr)
        return 2
    runners = {
        "generate": generate.run, "explore": explore.run, "aggregate": aggregate.run,
        "window": window.run, "explain": explain.run, "save": save.run,
        "timezone": timezone.run,
    }
    ctx = Context(spark=spark, workdir=workdir, rows=args.rows, seed=args.seed, lang=lang)
    try:
        if args.command == "tour":
            print(t(lang, "tour.intro", rows=f"{args.rows:,}", seed=args.seed))
            for number, name in enumerate(STEPS, start=1):
                ctx.step_no = number
                runners[name](ctx)
            print("\n" + t(lang, "tour.done", out=workdir / "output"))
        else:
            ctx.step_no = STEPS.index(args.command) + 1 if args.command in STEPS else 0
            runners[args.command](ctx)
        if args.ui:
            _wait_for_ui(lang)
    finally:
        spark.stop()  # always release the JVM, even if a step failed
    return 0


if __name__ == "__main__":
    sys.exit(main())

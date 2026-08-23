"""
run_dbt.py

Runs the dbt medallion pipeline in dependency order:

    1. dbt run  --select silver
    2. dbt test --select silver
    3. dbt run  --select gold
    4. dbt test --select gold

Stops at the first failure by default. Use --continue-on-error
to run all steps.

The script automatically detects the dbt project by locating
dbt_project.yml near the script location. You can also specify
the project manually using --project-dir.

Defaults:
    --select silver
    --select gold

Supports:
    --project-dir
    --silver-select
    --gold-select
    --dry-run
    --continue-on-error
    --full-refresh

Any arguments after `--` are passed to every dbt command.

Examples:
    uv run run_dbt.py
    uv run run_dbt.py --project-dir ./DBT_databricks
    uv run run_dbt.py --dry-run
    uv run run_dbt.py --full-refresh
    uv run run_dbt.py -- --target prod

Exit code is 0 only when all pipeline steps succeed.

Author: Nitin
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich import box

STATUS_STYLE = {"PASS": "bold green", "FAIL": "bold red", "SKIPPED": "dim"}


# --------------------------------------------------------------------------
# Dynamic discovery helpers
# --------------------------------------------------------------------------

def _walk_up(start: Path, max_depth: int = 6) -> list[Path]:
    """Return start, its parent, its parent's parent, ... up to max_depth."""
    roots = []
    current = start.resolve()
    for _ in range(max_depth):
        roots.append(current)
        if current.parent == current:  # reached filesystem root
            break
        current = current.parent
    return roots


def discover_dbt_project_dir(script_path: Path) -> "Path | None":
    """
    Find the folder that contains dbt_project.yml, without assuming any
    fixed relative layout or folder name.

    Walks up from the script location. At each level, checks the level
    itself, then its immediate subfolders (subfolders with "dbt" in the
    name - e.g. DBT_databricks - are checked first, so this "just works"
    even if there happen to be multiple candidate folders).
    """
    for root in _walk_up(script_path.parent):
        if (root / "dbt_project.yml").is_file():
            return root
        if root.is_dir():
            candidates = sorted(
                (d for d in root.iterdir() if d.is_dir() and (d / "dbt_project.yml").is_file()),
                key=lambda d: ("dbt" not in d.name.lower(), d.name.lower()),
            )
            if candidates:
                return candidates[0]
    return None


def discover_utils_dir(script_path: Path) -> "Path | None":
    """Same walk-up strategy, but looking for a utils/logger.py."""
    for root in _walk_up(script_path.parent):
        direct = root / "utils"
        if (direct / "logger.py").is_file():
            return direct
        if root.is_dir():
            for d in root.iterdir():
                if d.is_dir() and (d / "utils" / "logger.py").is_file():
                    return d / "utils"
    return None


def load_get_logger(script_path: Path):
    """
    Import get_logger from the project's utils/logger.py if we can find it;
    otherwise fall back to a plain stdlib logger so this script still runs
    standalone (e.g. if utils/ lives somewhere this search can't reach).
    """
    utils_dir = discover_utils_dir(script_path)
    if utils_dir is not None:
        if str(utils_dir) not in sys.path:
            sys.path.insert(0, str(utils_dir))
        try:
            from logger import get_logger  # type: ignore
            return get_logger, utils_dir
        except ImportError:
            pass

    import logging as _logging

    def _fallback_get_logger(stage: str = "transformation", name: str = "dbt_pipeline"):
        lg = _logging.getLogger(name)
        if not lg.handlers:
            handler = _logging.StreamHandler()
            handler.setFormatter(_logging.Formatter(f"[%(asctime)s] [{stage}] %(levelname)s: %(message)s"))
            lg.addHandler(handler)
            lg.setLevel(_logging.INFO)
        return lg

    return _fallback_get_logger, None


get_logger, _resolved_utils_dir = load_get_logger(Path(__file__))


@dataclass
class StepResult:
    label: str
    command: list[str]
    status: str = "SKIPPED"  # PASS | FAIL | SKIPPED
    duration_s: float = 0.0
    returncode: "int | None" = None


def build_steps(args, extra_args: list[str]) -> list[tuple[str, list[str]]]:
    full_refresh = ["--full-refresh"] if args.full_refresh else []
    return [
        ("Build silver models", ["dbt", "run", "--select", args.silver_select] + full_refresh + extra_args),
        ("Test silver models", ["dbt", "test", "--select", args.silver_select] + extra_args),
        ("Build gold models", ["dbt", "run", "--select", args.gold_select] + full_refresh + extra_args),
        ("Test gold models", ["dbt", "test", "--select", args.gold_select] + extra_args),
    ]


def run_step(label: str, command: list[str], project_dir: Path, console: Console) -> StepResult:
    console.print(Rule(f"[bold blue]{label}[/bold blue]"))
    console.print(f"[dim]$ {' '.join(command)}[/dim]\n")

    start = time.perf_counter()
    proc = subprocess.run(command, cwd=str(project_dir))  # inherits stdout/stderr so dbt's own output streams live
    duration = time.perf_counter() - start

    status = "PASS" if proc.returncode == 0 else "FAIL"
    console.print()
    return StepResult(label=label, command=command, status=status, duration_s=duration, returncode=proc.returncode)


def render_summary(console: Console, results: list[StepResult]) -> bool:
    table = Table(title="Pipeline Summary", box=box.ROUNDED, title_justify="left")
    table.add_column("Step")
    table.add_column("Status", justify="center")
    table.add_column("Duration", justify="right")

    for r in results:
        style = STATUS_STYLE.get(r.status, "")
        duration = f"{r.duration_s:.1f}s" if r.status != "SKIPPED" else "-"
        table.add_row(r.label, f"[{style}]{r.status}[/{style}]", duration)

    console.print(table)

    ok = all(r.status == "PASS" for r in results if r.status != "SKIPPED") and any(
        r.status == "PASS" for r in results
    )
    total_duration = sum(r.duration_s for r in results)
    message = f"finished in {total_duration:.1f}s"
    console.print(Panel(message, style="bold green" if ok else "bold red", border_style="green" if ok else "red"))
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run dbt silver models -> silver tests -> gold models -> gold tests, in order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--project-dir", type=Path, default=None,
        help="dbt project root containing dbt_project.yml. If omitted, this is auto-detected by "
             "searching near this script - no need to move dbt_project.yml or rename your dbt "
             "folder (e.g. DBT_databricks) to make it findable."
    )
    parser.add_argument("--silver-select", default="silver",
                         help="dbt --select value for the silver layer (default: silver)")
    parser.add_argument("--gold-select", default="gold",
                         help="dbt --select value for the gold layer (default: gold)")
    parser.add_argument("--full-refresh", action="store_true",
                         help="Pass --full-refresh to both `dbt run` steps")
    parser.add_argument("--continue-on-error", action="store_true",
                         help="Run every step even if an earlier one fails (default: stop at first failure)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print the dbt commands that would run without executing them")
    parser.add_argument("extra_args", nargs=argparse.REMAINDER,
                         help="Anything after -- is appended verbatim to every dbt command, "
                              "e.g. -- --target prod --vars '{key: value}'")
    args = parser.parse_args()

    # argparse.REMAINDER keeps a leading "--" if present; strip it.
    extra_args = args.extra_args[1:] if args.extra_args[:1] == ["--"] else args.extra_args

    console = Console()
    console.rule("[bold blue]dbt Silver -> Gold Pipeline")

    logger = get_logger(stage="transformation", name="dbt_silver_gold_pipeline")
    if _resolved_utils_dir is not None:
        logger.info(f"Using logger from {_resolved_utils_dir}")
    else:
        console.print("[dim]No utils/logger.py found nearby - using a plain stdlib logger instead.[/dim]")
    logger.info("Starting dbt silver -> gold pipeline")

    # Resolve the dbt project dir: explicit flag wins, otherwise auto-detect.
    if args.project_dir is not None:
        project_dir = args.project_dir.resolve()
        if not (project_dir / "dbt_project.yml").is_file():
            console.print(f"[bold red]--project-dir does not contain a dbt_project.yml: {project_dir}[/bold red]")
            logger.error(f"--project-dir does not contain a dbt_project.yml: {project_dir}")
            return 1
    else:
        discovered = discover_dbt_project_dir(Path(__file__))
        if discovered is None:
            console.print(
                "[bold red]Could not auto-detect a dbt project.[/bold red] No dbt_project.yml was found "
                "in this script's folder, its parents, or their immediate subfolders.\n"
                "Pass it explicitly, e.g. --project-dir ./DBT_databricks"
            )
            logger.error("dbt_project.yml not found during auto-detection")
            return 1
        project_dir = discovered
        console.print(f"[dim]Auto-detected dbt project dir: {project_dir}[/dim]")

    logger.info(f"Using dbt project dir: {project_dir}")

    if shutil.which("dbt") is None and not args.dry_run:
        console.print("[bold red]`dbt` was not found on PATH.[/bold red] "
                       "Activate the environment that has dbt-core (and your adapter) installed, then retry.")
        logger.error("`dbt` not found on PATH")
        return 1

    steps = build_steps(args, extra_args)

    if args.dry_run:
        console.print("[bold yellow]--dry-run: commands that would run, in order:[/bold yellow]\n")
        for label, command in steps:
            console.print(f"[bold]{label}[/bold]")
            console.print(f"  $ {' '.join(command)}  [dim](cwd: {project_dir})[/dim]\n")
        return 0

    results: list[StepResult] = []
    for label, command in steps:
        result = run_step(label, command, project_dir, console)
        results.append(result)
        logger.info(f"{label} -> {result.status} ({result.duration_s:.1f}s, exit code {result.returncode})")

        if result.status == "FAIL":
            logger.error(f"{label} failed with exit code {result.returncode}")
            if not args.continue_on_error:
                console.print(f"[bold red]Stopping pipeline: '{label}' failed.[/bold red] "
                               f"Pass --continue-on-error to run remaining steps anyway.\n")
                break

    # Any step not attempted (because we stopped early) is marked SKIPPED for the summary.
    attempted_labels = {r.label for r in results}
    for label, command in steps:
        if label not in attempted_labels:
            results.append(StepResult(label=label, command=command, status="SKIPPED"))

    ok = render_summary(console, results)
    if ok:
        logger.info("Pipeline completed: all steps passed")
    else:
        logger.error("Pipeline failed: see console/log for details")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
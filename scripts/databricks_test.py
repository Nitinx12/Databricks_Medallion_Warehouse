"""
databricks_tests.py

Runs data-quality SQL tests for the bronze, silver, and gold layers in
Databricks and generates a Rich console report.

Test convention:
    - 0 rows returned -> PASS
    - Rows returned   -> FAIL
    - Query exception -> ERROR

The script supports compound SQL statements, although the Databricks SQL
connector may only expose the final result set when multiple SELECT
statements are produced.

Features:
    - Run all layers or selected layers
    - Dry-run mode
    - JSON test report
    - Fail-fast option
    - Automatic Databricks connection and project logging

Expected project structure:

    project_root/
    ├── scripts/
    │   └── databricks_tests.py
    ├── utils/
    │   ├── connection.py
    │   ├── logger.py
    │   └── engine.py
    └── tests/
        ├── bronze/
        ├── silver/
        └── gold/

Usage:
    uv run databricks_tests.py
    uv run databricks_tests.py --layer silver gold
    uv run databricks_tests.py --dry-run
    uv run databricks_tests.py --json-report out.json
    uv run databricks_tests.py --fail-fast

Exit code 0 means all tests passed.

Author: Nitin
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

try:
    from dotenv import load_dotenv  # optional convenience, not a hard dependency

    load_dotenv()
except ImportError:
    pass

# connection.py / logger.py (and the engine.py they in turn rely on) live in
# utils/ at the project root, not next to this script - and connection.py
# imports engine.py with a bare `from engine import ...`, so it needs utils/
# itself on sys.path (not the project root) for both imports to resolve.
_UTILS_DIR = Path(__file__).resolve().parent.parent / "utils"
if str(_UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(_UTILS_DIR))

from connection import get_databricks_connection
from logger import get_logger

LAYERS = ("bronze", "silver", "gold")
LAYER_STYLE = {"bronze": "orange3", "silver": "grey70", "gold": "gold1"}
STATUS_STYLE = {"PASS": "bold green", "FAIL": "bold red", "ERROR": "bold yellow", "SKIPPED": "dim"}


@dataclass
class TestResult:
    layer: str
    file_name: str
    status: str  # PASS | FAIL | ERROR | SKIPPED
    row_count: int = 0
    duration_s: float = 0.0
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_json(self) -> dict:
        return {
            "layer": self.layer,
            "file": self.file_name,
            "status": self.status,
            "row_count": self.row_count,
            "duration_s": round(self.duration_s, 3),
            "error": self.error_message,
        }


def discover_tests(tests_dir: Path, layers: tuple[str, ...]) -> dict[str, list[Path]]:
    discovered: dict[str, list[Path]] = {}
    for layer in layers:
        layer_dir = tests_dir / layer
        if not layer_dir.is_dir():
            discovered[layer] = []
            continue
        discovered[layer] = sorted(layer_dir.glob("*.sql"))
    return discovered


def run_one_test(cursor, sql_text: str, layer: str, file_name: str) -> TestResult:
    start = time.perf_counter()
    try:
        cursor.execute(sql_text)
        if cursor.description is None:
            columns, rows = [], []
        else:
            columns = [d[0] for d in cursor.description]
            rows = [tuple(r) for r in cursor.fetchall()]
        duration = time.perf_counter() - start
        status = "FAIL" if rows else "PASS"
        return TestResult(
            layer=layer,
            file_name=file_name,
            status=status,
            row_count=len(rows),
            duration_s=duration,
            columns=columns,
            rows=rows,
        )
    except Exception as exc:  # noqa: BLE001 - we want to capture and report, not crash the run
        duration = time.perf_counter() - start
        return TestResult(
            layer=layer,
            file_name=file_name,
            status="ERROR",
            duration_s=duration,
            error_message=str(exc),
        )


def _log_result(logger, result: TestResult) -> None:
    """Mirror a TestResult into the logger.py-backed run log."""
    message = (
        f"[{result.layer}] {result.file_name} -> {result.status} "
        f"({result.row_count} row(s), {result.duration_s:.2f}s)"
    )
    if result.status == "PASS":
        logger.info(message)
    elif result.status == "FAIL":
        logger.warning(message)
    else:
        logger.error(f"{message} | {result.error_message}")


def render_layer_table(console: Console, layer: str, results: list[TestResult]) -> None:
    if not results:
        console.print(f"[dim]  no .sql files found for {layer}[/dim]")
        return

    table = Table(box=box.SIMPLE_HEAVY, show_lines=False, pad_edge=False)
    table.add_column("Test", style="cyan", no_wrap=True)
    table.add_column("Status", justify="center")
    table.add_column("Rows", justify="right")
    table.add_column("Duration", justify="right")

    for r in results:
        style = STATUS_STYLE.get(r.status, "")
        table.add_row(
            r.file_name,
            f"[{style}]{r.status}[/{style}]",
            str(r.row_count) if r.status != "SKIPPED" else "-",
            f"{r.duration_s:.2f}s" if r.status != "SKIPPED" else "-",
        )
    console.print(table)


def render_failure_detail(console: Console, r: TestResult) -> None:
    if r.status == "FAIL":
        detail = Table(
            title=f"[bold red]FAIL[/bold red] -> {r.file_name} ({r.row_count} row(s))",
            box=box.MINIMAL_DOUBLE_HEAD,
            title_justify="left",
        )
        for col in r.columns:
            detail.add_column(str(col))
        for row in r.rows[:25]:  # cap what we print; row_count above still shows the true total
            detail.add_row(*[str(v) for v in row])
        console.print(detail)
        if r.row_count > 25:
            console.print(f"  [dim]... {r.row_count - 25} more row(s) not shown[/dim]")
    elif r.status == "ERROR":
        console.print(
            Panel(
                r.error_message or "unknown error",
                title=f"[bold yellow]ERROR[/bold yellow] -> {r.file_name}",
                border_style="yellow",
                title_align="left",
            )
        )


def render_summary(console: Console, all_results: list[TestResult], total_duration: float) -> None:
    by_layer: dict[str, list[TestResult]] = {layer: [] for layer in LAYERS}
    for r in all_results:
        by_layer.setdefault(r.layer, []).append(r)

    summary = Table(title="Summary", box=box.ROUNDED, title_justify="left")
    summary.add_column("Layer")
    summary.add_column("Passed", justify="right", style="green")
    summary.add_column("Failed", justify="right", style="red")
    summary.add_column("Errors", justify="right", style="yellow")
    summary.add_column("Total", justify="right")

    total_pass = total_fail = total_error = 0
    for layer in LAYERS:
        results = by_layer.get(layer, [])
        p = sum(1 for r in results if r.status == "PASS")
        f = sum(1 for r in results if r.status == "FAIL")
        e = sum(1 for r in results if r.status == "ERROR")
        total_pass += p
        total_fail += f
        total_error += e
        if results:
            summary.add_row(layer, str(p), str(f), str(e), str(len(results)))

    console.print(summary)

    total = total_pass + total_fail + total_error
    ok = total_fail == 0 and total_error == 0
    message = (
        f"{total_pass}/{total} passed, {total_fail} failed, {total_error} errored "
        f"-- finished in {total_duration:.1f}s"
    )
    console.print(
        Panel(
            message,
            style="bold green" if ok else "bold red",
            border_style="green" if ok else "red",
        )
    )
    return ok


def main() -> int:
    default_tests_dir = Path(__file__).resolve().parent.parent / "tests"

    parser = argparse.ArgumentParser(
        description="Run Databricks data-quality SQL tests and print a rich report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--tests-dir", type=Path, default=default_tests_dir,
                         help=f"Folder containing bronze/silver/gold subfolders (default: {default_tests_dir})")
    parser.add_argument("--layer", nargs="+", choices=LAYERS, default=list(LAYERS),
                         help="Only run these layers (default: all three)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Discover and list tests without connecting to Databricks")
    parser.add_argument("--fail-fast", action="store_true",
                         help="Stop after the first FAIL or ERROR")
    parser.add_argument("--json-report", type=Path, default=None,
                         help="Also write a machine-readable JSON report to this path")
    args = parser.parse_args()

    console = Console()
    console.rule("[bold blue]Databricks Data Quality Test Suite")

    logger = get_logger(stage="transformation", name="databricks_data_quality_tests")
    logger.info("Starting Databricks data-quality test suite")

    discovered = discover_tests(args.tests_dir, tuple(args.layer))
    total_files = sum(len(v) for v in discovered.values())
    if total_files == 0:
        console.print(f"[bold red]No .sql files found under {args.tests_dir}[/bold red]")
        logger.error(f"No .sql files found under {args.tests_dir}")
        return 1

    console.print(f"[dim]tests dir:[/dim] {args.tests_dir}")
    console.print(f"[dim]layers:[/dim] {', '.join(args.layer)}  [dim]|[/dim]  [dim]tests found:[/dim] {total_files}")

    all_results: list[TestResult] = []
    start = time.perf_counter()

    if args.dry_run:
        console.print("\n[bold yellow]--dry-run: not connecting to Databricks[/bold yellow]\n")
        logger.info(f"--dry-run: listing {total_files} test(s) without connecting")
        for layer in args.layer:
            console.print(f"[bold {LAYER_STYLE[layer]}]{layer.upper()}[/bold {LAYER_STYLE[layer]}]")
            for f in discovered[layer]:
                all_results.append(TestResult(layer=layer, file_name=f.name, status="SKIPPED"))
            render_layer_table(console, layer, [r for r in all_results if r.layer == layer])
        total_duration = time.perf_counter() - start
        render_summary(console, all_results, total_duration)
        return 0

    logger.info(f"Running {total_files} test(s) across layers: {', '.join(args.layer)}")

    try:
        with get_databricks_connection() as connection:
            cursor = connection.cursor()
            try:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TimeElapsedColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("Running tests...", total=total_files)
                    stop = False
                    for layer in args.layer:
                        if stop:
                            break
                        for f in discovered[layer]:
                            progress.update(task, description=f"[{layer}] {f.name}")
                            sql_text = f.read_text(encoding="utf-8")
                            result = run_one_test(cursor, sql_text, layer, f.name)
                            all_results.append(result)
                            _log_result(logger, result)
                            progress.advance(task)
                            if args.fail_fast and result.status in ("FAIL", "ERROR"):
                                stop = True
                                break
            finally:
                cursor.close()
    except Exception as exc:
        logger.error(f"Test run aborted: {exc}")
        console.print(f"[bold red]Databricks connection/run failed: {exc}[/bold red]")
        return 1

    total_duration = time.perf_counter() - start

    for layer in args.layer:
        layer_results = [r for r in all_results if r.layer == layer]
        if not layer_results:
            continue
        console.print(f"\n[bold {LAYER_STYLE[layer]}]{layer.upper()} layer[/bold {LAYER_STYLE[layer]}]")
        render_layer_table(console, layer, layer_results)
        for r in layer_results:
            if r.status in ("FAIL", "ERROR"):
                render_failure_detail(console, r)

    console.print()
    ok = render_summary(console, all_results, total_duration)

    if ok:
        logger.info(f"Test suite passed: {len(all_results)} test(s) in {total_duration:.1f}s")
    else:
        logger.error(f"Test suite failed after {total_duration:.1f}s - see console/log for details")

    if args.json_report:
        args.json_report.write_text(
            json.dumps([r.to_json() for r in all_results], indent=2), encoding="utf-8"
        )
        console.print(f"[dim]JSON report written to {args.json_report}[/dim]")
        logger.info(f"JSON report written to {args.json_report}")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
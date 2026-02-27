from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import click

from moldockpipe import engine
from moldockpipe.purge import purge_project
from moldockpipe.run_ui import render_final_summary, watch_run_status


@click.group(help="MolDockPipe orchestrator CLI")
def app():
    pass


def _emit_and_exit(result: dict) -> None:
    click.echo(json.dumps(result, indent=2))
    raise click.exceptions.Exit(code=int(result.get("exit_code", 0)))


def _ui_enabled(no_ui: bool) -> bool:
    if no_ui:
        return False
    return bool(getattr(sys.stdout, "isatty", lambda: False)())


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
@click.option("--no-ui", is_flag=True, default=False, help="Disable live terminal UI and print JSON only.")
@click.option("--force", is_flag=True, default=False, help="Rerun all rows regardless of manifest state.")
@click.option("--rerun-failed-only", is_flag=True, default=False, help="Rerun only rows marked FAILED for each stage.")
@click.option("--from-module", type=click.IntRange(1,4), default=1, show_default=True, help="Start planning from module N.")
def run(project_dir: Path, docking_mode: str, no_ui: bool, force: bool, rerun_failed_only: bool, from_module: int):
    if not _ui_enabled(no_ui):
        _emit_and_exit(engine.run(project_dir, {"docking_mode": docking_mode}, force=force, rerun_failed_only=rerun_failed_only, from_module=from_module))

    cmd = [
        sys.executable,
        "-m",
        "moldockpipe.cli",
        "_run-engine",
        str(project_dir),
        "--docking-mode",
        docking_mode,
        "--from-module",
        str(from_module),
    ]
    if force:
        cmd.append("--force")
    if rerun_failed_only:
        cmd.append("--rerun-failed-only")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    status_path = project_dir.resolve() / "state" / "run_status.json"

    # Read-only watcher: engine child remains single source of state updates.
    watch_run_status(status_path, poll_interval_s=1.0)

    stdout, stderr = proc.communicate()
    result = None
    if stdout:
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError:
            result = None

    if result is None:
        result = {
            "exit_code": int(proc.returncode or 1),
            "error": "Engine subprocess did not return JSON output.",
            "stderr": (stderr or "").strip(),
        }

    render_final_summary(result)
    if stderr and stderr.strip():
        click.echo("\n[engine stderr]", err=True)
        click.echo(stderr.strip(), err=True)
    raise click.exceptions.Exit(code=int(proc.returncode if proc.returncode is not None else result.get("exit_code", 1)))


@app.command(name="_run-engine", hidden=True)
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
@click.option("--force", is_flag=True, default=False)
@click.option("--rerun-failed-only", is_flag=True, default=False)
@click.option("--from-module", type=click.IntRange(1,4), default=1)
def run_engine(project_dir: Path, docking_mode: str, force: bool, rerun_failed_only: bool, from_module: int):
    # Internal entrypoint used by the live UI parent process.
    _emit_and_exit(engine.run(project_dir, {"docking_mode": docking_mode}, force=force, rerun_failed_only=rerun_failed_only, from_module=from_module))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit machine-readable JSON.")
def validate(project_dir: Path, docking_mode: str, as_json: bool):
    result = engine.validate_project(project_dir, {"docking_mode": docking_mode})
    _emit_and_exit(result if as_json else result)


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
def resume(project_dir: Path):
    _emit_and_exit(engine.resume(project_dir))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit machine-readable JSON.")
def status(project_dir: Path, as_json: bool):
    result = engine.status(project_dir)
    if as_json:
        _emit_and_exit(result)
    click.echo(json.dumps(result, indent=2))




@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit machine-readable JSON.")
def plan(project_dir: Path, docking_mode: str, as_json: bool):
    result = engine.plan(project_dir, {"docking_mode": docking_mode})
    if as_json:
        _emit_and_exit(result)
    click.echo(json.dumps(result, indent=2))

@app.command("export-report")
@click.argument("project_dir", type=click.Path(path_type=Path))
def export_report(project_dir: Path):
    click.echo(json.dumps(engine.export_report(project_dir), indent=2))


@app.command()
@click.argument("project_dir", required=False, type=click.Path(path_type=Path))
@click.option("--confirm", "confirm1", default=None, help="Internal use: confirmation token.", hidden=True)
@click.option("--confirm2", "confirm2", default=None, help="Internal use: confirmation token.", hidden=True)
def purge(project_dir: Path | None, confirm1: str | None, confirm2: str | None):
    """Purge a project folder for a fresh run (destructive)."""
    base = project_dir or Path(".")
    _emit_and_exit(purge_project(base, confirm1=confirm1, confirm2=confirm2))


def main() -> None:
    # Enables ANSI escape handling for clear-screen updates on supported Windows terminals.
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    app()


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import click

from moldockpipe import engine
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
def run(project_dir: Path, docking_mode: str, no_ui: bool):
    if not _ui_enabled(no_ui):
        _emit_and_exit(engine.run(project_dir, {"docking_mode": docking_mode}))

    cmd = [
        sys.executable,
        "-m",
        "moldockpipe.cli",
        "_run-engine",
        str(project_dir),
        "--docking-mode",
        docking_mode,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    status_path = project_dir.resolve() / "state" / "run_status.json"

    # Read-only watcher: engine child remains single source of state updates.
    watch_run_status(status_path, poll_interval_s=0.35)

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
def run_engine(project_dir: Path, docking_mode: str):
    # Internal entrypoint used by the live UI parent process.
    _emit_and_exit(engine.run(project_dir, {"docking_mode": docking_mode}))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
def validate(project_dir: Path, docking_mode: str):
    _emit_and_exit(engine.validate(project_dir, {"docking_mode": docking_mode}))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
def resume(project_dir: Path):
    _emit_and_exit(engine.resume(project_dir))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
def status(project_dir: Path):
    click.echo(json.dumps(engine.status(project_dir), indent=2))


@app.command("export-report")
@click.argument("project_dir", type=click.Path(path_type=Path))
def export_report(project_dir: Path):
    click.echo(json.dumps(engine.export_report(project_dir), indent=2))


def main() -> None:
    # Enables ANSI escape handling for clear-screen updates on supported Windows terminals.
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    app()


if __name__ == "__main__":
    main()

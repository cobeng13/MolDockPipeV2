from __future__ import annotations

import json
from pathlib import Path

import click

from moldockpipe import engine


@click.group(help="MolDockPipe orchestrator CLI")
def app():
    pass


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
def run(project_dir: Path, docking_mode: str):
    result = engine.run(project_dir, {"docking_mode": docking_mode})
    click.echo(json.dumps(result, indent=2))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
@click.option("--docking-mode", default="cpu", type=click.Choice(["cpu", "gpu"], case_sensitive=False))
def validate(project_dir: Path, docking_mode: str):
    result = engine.validate(project_dir, {"docking_mode": docking_mode})
    click.echo(json.dumps(result, indent=2))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
def resume(project_dir: Path):
    result = engine.resume(project_dir)
    click.echo(json.dumps(result, indent=2))


@app.command()
@click.argument("project_dir", type=click.Path(path_type=Path))
def status(project_dir: Path):
    result = engine.status(project_dir)
    click.echo(json.dumps(result, indent=2))


@app.command("export-report")
@click.argument("project_dir", type=click.Path(path_type=Path))
def export_report(project_dir: Path):
    result = engine.export_report(project_dir)
    click.echo(json.dumps(result, indent=2))


def main() -> None:
    app()


if __name__ == "__main__":
    main()

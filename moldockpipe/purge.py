from __future__ import annotations

import json
from pathlib import Path

import click

from moldockpipe.state.manifest import MANIFEST_FIELDS


FOLDERS_TO_CLEAN = [
    "input",
    "output",
    "3D_Structures",
    "prepared_ligands",
    "results",
    "state",
    "logs",
]

DELETE_EXTS = {".smi", ".sdf", ".pdbqt", ".log", ".tmp"}

KEEP_FILES = {"VinaConfig.txt"}

CSV_HEADERS = {
    "state/manifest.csv": list(MANIFEST_FIELDS),
    "results/summary.csv": [
        "id", "inchikey", "vina_score", "pose_path", "created_at",
    ],
    "results/leaderboard.csv": [
        "rank", "id", "inchikey", "vina_score", "pose_path",
    ],
}


def truncate_or_create_csv(file: Path, headers: list[str]) -> None:
    file.parent.mkdir(parents=True, exist_ok=True)
    existed = file.exists()
    file.write_text(",".join(headers) + "\n", encoding="utf-8")
    action = "Truncated" if existed else "Created new"
    click.echo(f"[CSV] {action}: {file}")


def reset_run_status(file: Path) -> None:
    file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "2.0",
        "phase": "not_started",
        "completed_modules": [],
        "failed_module": None,
        "started_at": None,
        "updated_at": None,
        "finished_at": None,
        "history": [],
    }
    file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    click.echo(f"[JSON] Reset: {file}")


def clean_folder(folder: Path) -> None:
    if not folder.exists() or not folder.is_dir():
        return
    for f in folder.glob("*"):
        if f.is_file():
            if f.suffix.lower() == ".csv":
                continue
            if f.name in KEEP_FILES:
                continue
            if f.suffix.lower() in DELETE_EXTS:
                click.echo(f"[DEL] {f}")
                try:
                    f.unlink()
                except Exception as exc:  # pragma: no cover
                    click.echo(f"  [WARN] Could not delete {f}: {exc}")
        elif f.is_dir():
            clean_folder(f)


def confirm_action(base: Path, *, confirm1: str | None = None, confirm2: str | None = None) -> None:
    click.echo(f"\nBase Directory: {base}")
    click.echo("This operation will:")
    click.echo(f" - Clean folders: {', '.join(FOLDERS_TO_CLEAN)}")
    click.echo(" - Delete .smi, .sdf, .pdbqt, .log, .tmp files")
    click.echo(" - Truncate or recreate manifest and result CSVs")
    click.echo(" - Reset run_status.json and clear logs\n")

    if confirm1 is None:
        confirm1 = click.prompt("Type 'yes' to continue", default="", show_default=False)
    if confirm1.strip().lower() != "yes":
        raise click.Abort()

    if confirm2 is None:
        confirm2 = click.prompt("Type 'yes' again to confirm", default="", show_default=False)
    if confirm2.strip().lower() != "yes":
        raise click.Abort()


def validate_project_dir(base: Path) -> None:
    config = base / "config" / "run.yml"
    input_csv = base / "input" / "input.csv"
    if not config.exists():
        raise click.ClickException(
            "Refusing to purge: directory does not look like a MolDockPipe project. "
            "Expected config/run.yml. "
            "Pass an explicit project directory."
        )
    if not input_csv.exists():
        click.echo(
            "[WARN] input/input.csv not found. Purge will continue, but a run will require this file.",
            err=True,
        )


def purge_project(base: Path, *, confirm1: str | None = None, confirm2: str | None = None) -> dict:
    base = base.resolve()
    validate_project_dir(base)
    confirm_action(base, confirm1=confirm1, confirm2=confirm2)

    for folder in FOLDERS_TO_CLEAN:
        clean_folder(base / folder)

    for rel, headers in CSV_HEADERS.items():
        truncate_or_create_csv(base / rel, headers)

    reset_run_status(base / "state" / "run_status.json")
    click.echo("\nPipeline cleaned. CSV headers preserved (or re-created), all other data cleared.")
    return {"ok": True, "exit_code": 0, "message": "purged", "project_dir": str(base)}

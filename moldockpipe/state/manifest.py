from __future__ import annotations

import csv
from pathlib import Path

MANIFEST_FIELDS = [
    "id",
    "smiles",
    "inchikey",
    "admet_status",
    "admet_reason",
    "sdf_status",
    "sdf_path",
    "sdf_reason",
    "pdbqt_status",
    "pdbqt_path",
    "pdbqt_reason",
    "vina_status",
    "vina_score",
    "vina_pose",
    "vina_reason",
    "config_hash",
    "receptor_sha1",
    "tools_rdkit",
    "tools_meeko",
    "tools_vina",
    "created_at",
    "updated_at",
]


def read_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def write_manifest(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in MANIFEST_FIELDS})

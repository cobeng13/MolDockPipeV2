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
    "sdf_fp",
    "sdf_rdkit_ver",
    "pdbqt_fp",
    "pdbqt_meeko_ver",
    "vina_fp",
    "vina_exe_sha1",
    "vina_receptor_sha1",
    "vina_config_hash",
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
        rows=[]
        for row in reader:
            cleaned={}
            for k,v in dict(row).items():
                sv="" if v is None else str(v)
                cleaned[k]="" if sv.strip().lower() in {"nan","none"} else sv
            for f in MANIFEST_FIELDS:
                cleaned.setdefault(f, "")
            rows.append(cleaned)
        return rows


def write_manifest(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in MANIFEST_FIELDS})

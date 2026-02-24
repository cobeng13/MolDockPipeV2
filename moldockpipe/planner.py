from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from moldockpipe.state import read_manifest


@dataclass
class WorkPlan:
    module1_ids: set[str]
    module2_ids: set[str]
    module3_ids: set[str]
    module4_ids: set[str]
    stats: dict


def is_admet_pass(value) -> bool:
    if value is None:
        return False
    s = str(value).strip().upper()
    return s in {"PASS", "PASSED", "OK", "TRUE", "1", "Y", "YES"}


def _input_ids(input_csv: Path) -> set[str]:
    ids: set[str] = set()
    if not input_csv.exists():
        return ids
    with input_csv.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            smiles = (row.get("smiles") or "").strip()
            if not smiles:
                continue
            rid = (row.get("id") or "").strip()
            if rid:
                ids.add(rid)
    return ids


def _exists_nonempty(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except OSError:
        return False


def _definitive_pose_path(project_dir: Path, lig_id: str, row: dict) -> Path:
    pose = (row.get("vina_pose") or "").strip()
    if pose:
        p = Path(pose)
        if not p.is_absolute():
            p = (project_dir / p).resolve()
        return p
    return (project_dir / "results" / f"{lig_id}_out.pdbqt").resolve()


def compute_work_plan(project_dir: Path) -> WorkPlan:
    """Compute deterministic stage-gated TODO IDs from manifest + filesystem.

    Phase 1 rules intentionally do not apply fingerprint staleness yet.
    """
    project_dir = project_dir.resolve()
    manifest_path = project_dir / "state" / "manifest.csv"
    rows = read_manifest(manifest_path)
    by_id = {str((r.get("id") or "")).strip(): r for r in rows if str((r.get("id") or "")).strip()}
    ids = _input_ids(project_dir / "input" / "input.csv")

    m1: set[str] = set()
    m2: set[str] = set()
    m3: set[str] = set()
    m4: set[str] = set()

    for lig_id in sorted(ids):
        row = by_id.get(lig_id, {})
        admet_status = str((row.get("admet_status") or "")).strip().upper()
        sdf_status = str((row.get("sdf_status") or "")).strip().upper()
        pdbqt_status = str((row.get("pdbqt_status") or "")).strip().upper()
        vina_status = str((row.get("vina_status") or "")).strip().upper()

        # Module 1: ADMET missing/invalid only.
        if admet_status not in {"PASS", "FAIL"}:
            m1.add(lig_id)

        # Module 2: requires ADMET PASS and missing/non-DONE/nonexistent SDF.
        sdf_path = (project_dir / "3D_Structures" / f"{lig_id}.sdf").resolve()
        if is_admet_pass(admet_status) and (sdf_status != "DONE" or not _exists_nonempty(sdf_path)):
            m2.add(lig_id)

        # Module 3: requires SDF DONE and missing/non-DONE/nonexistent PDBQT.
        pdbqt_path = (project_dir / "prepared_ligands" / f"{lig_id}.pdbqt").resolve()
        if sdf_status == "DONE" and (pdbqt_status != "DONE" or not _exists_nonempty(pdbqt_path)):
            m3.add(lig_id)

        # Module 4: requires PDBQT DONE and missing/non-DONE/nonexistent pose.
        pose_path = _definitive_pose_path(project_dir, lig_id, row)
        if pdbqt_status == "DONE" and (vina_status != "DONE" or not _exists_nonempty(pose_path)):
            m4.add(lig_id)

    return WorkPlan(
        module1_ids=m1,
        module2_ids=m2,
        module3_ids=m3,
        module4_ids=m4,
        stats={
            "input_ids": len(ids),
            "module1_todo": len(m1),
            "module2_todo": len(m2),
            "module3_todo": len(m3),
            "module4_todo": len(m4),
            # Future extension point for fingerprint-driven stale detection.
            "stale_counts": {"module2": 0, "module3": 0, "module4": 0},
        },
    )

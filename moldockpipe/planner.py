from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from moldockpipe.artifacts import pdbqt_path, sdf_path, vina_out_path
from moldockpipe.fingerprints import pdbqt_fp, sdf_fp, sha1_file, vina_fp
from moldockpipe.state import read_manifest


@dataclass
class WorkPlan:
    module1_ids: set[str]
    module2_ids: set[str]
    module3_ids: set[str]
    module4_ids: set[str]
    stats: dict
    reasons: dict[str, dict[str, set[str]]]


def is_admet_pass(value) -> bool:
    if value is None:
        return False
    s = str(value).strip().upper()
    return s in {"PASS", "PASSED", "OK", "TRUE", "1", "Y", "YES"}


def _input_rows(input_csv: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not input_csv.exists():
        return out
    with input_csv.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rid = (row.get("id") or "").strip()
            smiles = (row.get("smiles") or "").strip()
            if rid and smiles:
                out[rid] = row
    return out


def _exists_nonempty(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except OSError:
        return False


def compute_work_plan(project_dir: Path, *, resolved: dict, versions: dict, config_hash: str, docking_params: dict) -> WorkPlan:
    project_dir = project_dir.resolve()
    rows = read_manifest(project_dir / "state" / "manifest.csv")
    by_id = {str((r.get("id") or "")).strip(): r for r in rows if str((r.get("id") or "")).strip()}
    inputs = _input_rows(project_dir / "input" / "input.csv")

    rdkit_ver = str(versions.get("rdkit") or "")
    meeko_ver = str(versions.get("meeko") or "")
    vina_ver = str(resolved.get("vina_gpu_path") or resolved.get("vina_cpu_path") or "")
    receptor_sha1 = ""
    rp = resolved.get("receptor_path")
    if rp and Path(rp).exists():
        receptor_sha1 = sha1_file(Path(rp))

    todo = {"module1": set(), "module2": set(), "module3": set(), "module4": set()}
    reasons = {
        "module2": {"status_not_done": set(), "missing": set(), "failed": set(), "stale": set()},
        "module3": {"status_not_done": set(), "missing": set(), "failed": set(), "stale": set()},
        "module4": {"status_not_done": set(), "missing": set(), "failed": set(), "stale": set()},
    }

    for lig_id, input_row in sorted(inputs.items()):
        row = by_id.get(lig_id, {})
        admet = str((row.get("admet_status") or "")).strip().upper()
        sdf_status = str((row.get("sdf_status") or "")).strip().upper()
        pdbqt_status = str((row.get("pdbqt_status") or "")).strip().upper()
        vina_status = str((row.get("vina_status") or "")).strip().upper()

        smiles = str(input_row.get("smiles") or "")
        cur_sdf_fp = sdf_fp(smiles, rdkit_ver, params={})
        cur_pdbqt_fp = pdbqt_fp(cur_sdf_fp, meeko_ver, params={})
        cur_vina_fp = vina_fp(cur_pdbqt_fp, vina_ver, receptor_sha1, docking_params, config_hash)

        # Module 1 gating
        if admet not in {"PASS", "FAIL"}:
            todo["module1"].add(lig_id)

        # Module 2
        if is_admet_pass(admet):
            s2 = False
            if sdf_status == "FAILED":
                reasons["module2"]["failed"].add(lig_id); s2 = True
            elif sdf_status != "DONE":
                reasons["module2"]["status_not_done"].add(lig_id); s2 = True
            elif not _exists_nonempty(sdf_path(project_dir, lig_id)):
                reasons["module2"]["missing"].add(lig_id); s2 = True
            elif str(row.get("sdf_fp") or "") != cur_sdf_fp:
                reasons["module2"]["stale"].add(lig_id); s2 = True
            if s2:
                todo["module2"].add(lig_id)

            # Module 3 (cascade from module2 invalidation)
            s3 = s2
            if not s3:
                if pdbqt_status == "FAILED":
                    reasons["module3"]["failed"].add(lig_id); s3 = True
                elif pdbqt_status != "DONE":
                    reasons["module3"]["status_not_done"].add(lig_id); s3 = True
                elif not _exists_nonempty(pdbqt_path(project_dir, lig_id)):
                    reasons["module3"]["missing"].add(lig_id); s3 = True
                elif str(row.get("pdbqt_fp") or "") != cur_pdbqt_fp:
                    reasons["module3"]["stale"].add(lig_id); s3 = True
            else:
                reasons["module3"]["stale"].add(lig_id)
            if s3:
                todo["module3"].add(lig_id)

            # Module 4 (cascade from 2/3)
            s4 = s2 or s3
            if not s4:
                if vina_status == "FAILED":
                    reasons["module4"]["failed"].add(lig_id); s4 = True
                elif vina_status != "DONE":
                    reasons["module4"]["status_not_done"].add(lig_id); s4 = True
                elif not _exists_nonempty(vina_out_path(project_dir, lig_id)):
                    reasons["module4"]["missing"].add(lig_id); s4 = True
                elif str(row.get("vina_fp") or "") != cur_vina_fp:
                    reasons["module4"]["stale"].add(lig_id); s4 = True
            else:
                reasons["module4"]["stale"].add(lig_id)
            if s4:
                todo["module4"].add(lig_id)

    stats = {
        "input_ids": len(inputs),
        "module1_todo": len(todo["module1"]),
        "module2_todo": len(todo["module2"]),
        "module3_todo": len(todo["module3"]),
        "module4_todo": len(todo["module4"]),
        "reasons": {
            mod: {k: len(v) for k, v in detail.items()} for mod, detail in reasons.items()
        },
        "samples": {
            mod: sorted(list(ids))[:10] for mod, ids in todo.items()
        },
    }

    return WorkPlan(
        module1_ids=todo["module1"],
        module2_ids=todo["module2"],
        module3_ids=todo["module3"],
        module4_ids=todo["module4"],
        stats=stats,
        reasons=reasons,
    )

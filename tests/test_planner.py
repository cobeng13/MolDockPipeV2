from pathlib import Path

from moldockpipe.fingerprints import pdbqt_fp, sdf_fp, vina_fp
from moldockpipe.planner import compute_work_plan
from moldockpipe.state import write_manifest


def _base_project(tmp_path: Path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "state").mkdir(parents=True)
    (tmp_path / "3D_Structures").mkdir(parents=True)
    (tmp_path / "prepared_ligands").mkdir(parents=True)
    (tmp_path / "results").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig1,CCO\nlig2,CCC\n", encoding="utf-8")


def test_planner_second_run_all_skipped(tmp_path):
    _base_project(tmp_path)
    (tmp_path / "3D_Structures" / "lig1.sdf").write_text("x", encoding="utf-8")
    (tmp_path / "3D_Structures" / "lig2.sdf").write_text("x", encoding="utf-8")
    (tmp_path / "prepared_ligands" / "lig1.pdbqt").write_text("x", encoding="utf-8")
    (tmp_path / "prepared_ligands" / "lig2.pdbqt").write_text("x", encoding="utf-8")
    (tmp_path / "results" / "lig1_out.pdbqt").write_text("x", encoding="utf-8")
    (tmp_path / "results" / "lig2_out.pdbqt").write_text("x", encoding="utf-8")

    write_manifest(
        tmp_path / "state" / "manifest.csv",
        [
            {"id": "lig1", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE", "sdf_fp": sdf_fp("CCO","r",{}), "pdbqt_fp": pdbqt_fp(sdf_fp("CCO","r",{}),"m",{}), "vina_fp": vina_fp(pdbqt_fp(sdf_fp("CCO","r",{}),"m",{}),"","",{},"cfg")},
            {"id": "lig2", "smiles": "CCC", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE", "sdf_fp": sdf_fp("CCC","r",{}), "pdbqt_fp": pdbqt_fp(sdf_fp("CCC","r",{}),"m",{}), "vina_fp": vina_fp(pdbqt_fp(sdf_fp("CCC","r",{}),"m",{}),"","",{},"cfg")},
        ],
    )

    plan = compute_work_plan(tmp_path, resolved={"receptor_path": None, "vina_cpu_path": "vina", "vina_gpu_path": None, "box": {}, "docking_params": {}}, versions={"rdkit": "r", "meeko": "m"}, config_hash="cfg", docking_params={})
    assert plan.module1_ids == set()
    assert plan.module2_ids == set()
    assert plan.module3_ids == set()
    assert plan.module4_ids == set()


def test_planner_deleted_pdbqt_only_hits_module3_and4(tmp_path):
    _base_project(tmp_path)
    (tmp_path / "3D_Structures" / "lig1.sdf").write_text("x", encoding="utf-8")
    (tmp_path / "3D_Structures" / "lig2.sdf").write_text("x", encoding="utf-8")
    (tmp_path / "prepared_ligands" / "lig2.pdbqt").write_text("x", encoding="utf-8")
    (tmp_path / "results" / "lig2_out.pdbqt").write_text("x", encoding="utf-8")

    write_manifest(
        tmp_path / "state" / "manifest.csv",
        [
            {"id": "lig1", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE", "sdf_fp": sdf_fp("CCO","r",{}), "pdbqt_fp": pdbqt_fp(sdf_fp("CCO","r",{}),"m",{}), "vina_fp": vina_fp(pdbqt_fp(sdf_fp("CCO","r",{}),"m",{}),"","",{},"cfg")},
            {"id": "lig2", "smiles": "CCC", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE", "sdf_fp": sdf_fp("CCC","r",{}), "pdbqt_fp": pdbqt_fp(sdf_fp("CCC","r",{}),"m",{}), "vina_fp": vina_fp(pdbqt_fp(sdf_fp("CCC","r",{}),"m",{}),"","",{},"cfg")},
        ],
    )

    plan = compute_work_plan(tmp_path, resolved={"receptor_path": None, "vina_cpu_path": "vina", "vina_gpu_path": None, "box": {}, "docking_params": {}}, versions={"rdkit": "r", "meeko": "m"}, config_hash="cfg", docking_params={})
    assert plan.module1_ids == set()
    assert plan.module2_ids == set()
    assert plan.module3_ids == {"lig1"}
    assert plan.module4_ids == {"lig1"}


def test_planner_marks_stale_on_docking_change(tmp_path):
    _base_project(tmp_path)
    (tmp_path / "3D_Structures" / "lig1.sdf").write_text("x", encoding="utf-8")
    (tmp_path / "prepared_ligands" / "lig1.pdbqt").write_text("x", encoding="utf-8")
    (tmp_path / "results" / "lig1_out.pdbqt").write_text("x", encoding="utf-8")
    write_manifest(tmp_path / "state" / "manifest.csv", [{"id": "lig1", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE", "sdf_fp": "a", "pdbqt_fp": "b", "vina_fp": "c"}])

    plan = compute_work_plan(tmp_path, resolved={"receptor_path": None, "vina_cpu_path": "vina", "vina_gpu_path": None, "box": {}, "docking_params": {}}, versions={"rdkit": "r", "meeko": "m"}, config_hash="new-cfg", docking_params={"energy_range": 9})
    assert "lig1" in plan.module4_ids

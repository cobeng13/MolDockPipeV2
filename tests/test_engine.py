import json
from pathlib import Path

from moldockpipe import engine


def _setup_project(tmp_path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\nlig2,CCC\n", encoding="utf-8")
    (tmp_path / "receptors").mkdir(parents=True)
    (tmp_path / "receptors" / "target_prepared.pdbqt").write_text("REMARK receptor\n", encoding="utf-8")


def _fake_contract(paths, raw_config, warnings):
    resolved = {
        "receptor_path": str((paths["project"] / "receptors" / "target_prepared.pdbqt").resolve()),
        "vina_cpu_path": str((paths["project"] / "vina.exe").resolve()),
        "vina_gpu_path": None,
        "box": {"center": [0.0, 0.0, 0.0], "size": [20.0, 20.0, 20.0]},
        "docking_params": {"exhaustiveness": 8, "num_modes": 9, "energy_range": 3},
    }
    return resolved, {"python": "3.11.9", "rdkit": "2025.3.6", "meeko": "0.6.1", "pandas": "2.2.3"}


def _ok_result(rc=0):
    class R:
        returncode = rc
        stdout_log = "stdout.log"
        stderr_log = "stderr.log"

        @property
        def ok(self):
            return self.returncode == 0

    return R()




def _prime_manifest_all_stages(tmp_path):
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    rows = [
        {"id": "lig", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE"},
        {"id": "lig2", "smiles": "CCC", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE"},
    ]
    (tmp_path / "3D_Structures").mkdir(exist_ok=True)
    (tmp_path / "prepared_ligands").mkdir(exist_ok=True)
    (tmp_path / "results").mkdir(exist_ok=True)
    for rid in ("lig", "lig2"):
        (tmp_path / "3D_Structures" / f"{rid}.sdf").write_text("x", encoding="utf-8")
        (tmp_path / "prepared_ligands" / f"{rid}.pdbqt").write_text("x", encoding="utf-8")
        (tmp_path / "results" / f"{rid}_out.pdbqt").write_text("x", encoding="utf-8")
    engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
def test_run_overwrite_archive_and_schema(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    def m1(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        for r in rows:
            r["admet_status"] = "PASS"
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m2(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "3D_Structures").mkdir(exist_ok=True)
        for r in rows:
            r["sdf_status"] = "DONE"
            (tmp_path / "3D_Structures" / f"{r['id']}.sdf").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m3(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "prepared_ligands").mkdir(exist_ok=True)
        for r in rows:
            r["pdbqt_status"] = "DONE"
            (tmp_path / "prepared_ligands" / f"{r['id']}.pdbqt").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m4(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "results").mkdir(exist_ok=True)
        for r in rows:
            r["vina_status"] = "DONE"
            (tmp_path / "results" / f"{r['id']}_out.pdbqt").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    monkeypatch.setattr(engine.admet, "run", m1)
    monkeypatch.setattr(engine.build3d, "run", m2)
    monkeypatch.setattr(engine.meeko, "run", m3)
    monkeypatch.setattr(engine.docking_cpu, "run", m4)

    first = engine.run(tmp_path, {"docking_mode": "cpu"})
    second = engine.run(tmp_path, {"docking_mode": "cpu"})

    assert first["exit_code"] == 0
    assert second["exit_code"] == 0
    assert first["status"]["run_id"] != second["status"]["run_id"]
    assert second["status"]["schema_version"] == "2.0"
    assert (tmp_path / "state" / "runs" / first["status"]["run_id"] / "run_status.json").exists()


def test_progress_and_module_timestamps(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    engine.write_manifest((tmp_path / "state" / "manifest.csv"), [{"id": "lig", "smiles": "CCO", "admet_status": ""}, {"id": "lig2", "smiles": "CCC", "admet_status": ""}])
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    def m1(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        for r in rows:
            r["admet_status"] = "PASS"
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m2(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "3D_Structures").mkdir(exist_ok=True)
        for r in rows:
            r["sdf_status"] = "DONE"
            (tmp_path / "3D_Structures" / f"{r['id']}.sdf").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m3(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "prepared_ligands").mkdir(exist_ok=True)
        for r in rows:
            r["pdbqt_status"] = "DONE"
            (tmp_path / "prepared_ligands" / f"{r['id']}.pdbqt").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m4(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "results").mkdir(exist_ok=True)
        for r in rows:
            r["vina_status"] = "DONE"
            (tmp_path / "results" / f"{r['id']}_out.pdbqt").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    monkeypatch.setattr(engine.admet, "run", m1)
    monkeypatch.setattr(engine.build3d, "run", m2)
    monkeypatch.setattr(engine.meeko, "run", m3)
    monkeypatch.setattr(engine.docking_cpu, "run", m4)

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    st = res["status"]
    assert st["progress"]["percent"] == 100
    assert len(st["history"]) == 4
    assert all(h.get("started_at") and h.get("ended_at") is not None for h in st["history"])


def test_completed_with_errors_semantics(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    (tmp_path / "state" / "manifest.csv").write_text(
        "id,smiles,inchikey,admet_status,admet_reason,sdf_status,sdf_path,sdf_reason,pdbqt_status,pdbqt_path,pdbqt_reason,vina_status,vina_score,vina_pose,vina_reason,config_hash,receptor_sha1,tools_rdkit,tools_meeko,tools_vina,created_at,updated_at\n"
        "lig,CCO,,PASS,,DONE,,,DONE,,,FAILED,,,No pose,,,,,,,\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    monkeypatch.setattr(engine.admet, "run", lambda *a, **k: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda *a, **k: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda *a, **k: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda *a, **k: _ok_result(2))

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["exit_code"] == 2
    assert res["status"]["completed_with_errors"] is True
    assert res["status"]["result"] == "partial_success"
    assert res["status"]["status"] == "completed"


def test_validation_failure_exit_code_3(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    def bad_contract(paths, raw_config, warnings):
        raise engine.PreflightError("Missing receptor")

    monkeypatch.setattr(engine, "_validate_contract", bad_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["exit_code"] == 3
    assert res["status"]["status"] == "validation_failed"


def test_run_fails_fast_when_rdkit_missing(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    def contract_ok(paths, raw_config, warnings):
        resolved = {
            "receptor_path": str((paths["project"] / "receptors" / "target_prepared.pdbqt").resolve()),
            "vina_cpu_path": str((paths["project"] / "vina.exe").resolve()),
            "vina_gpu_path": None,
            "box": {"center": [0.0, 0.0, 0.0], "size": [20.0, 20.0, 20.0]},
            "docking_params": {"exhaustiveness": 8, "num_modes": 9, "energy_range": 3},
        }
        return resolved, {"python": "3.11.9", "rdkit": None, "meeko": "0.6.1", "pandas": "2.2.3"}

    monkeypatch.setattr(engine, "_validate_contract", contract_ok)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is False
    assert res["exit_code"] == 3
    assert res["status"]["status"] == "validation_failed"
    assert "Missing required Python packages for run: rdkit" in res["error"]


def test_config_snapshot_raw_and_hash_stable(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    def m1(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        for r in rows:
            r["admet_status"] = "PASS"
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m2(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "3D_Structures").mkdir(exist_ok=True)
        for r in rows:
            r["sdf_status"] = "DONE"
            (tmp_path / "3D_Structures" / f"{r['id']}.sdf").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m3(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "prepared_ligands").mkdir(exist_ok=True)
        for r in rows:
            r["pdbqt_status"] = "DONE"
            (tmp_path / "prepared_ligands" / f"{r['id']}.pdbqt").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m4(*a, **k):
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "results").mkdir(exist_ok=True)
        for r in rows:
            r["vina_status"] = "DONE"
            (tmp_path / "results" / f"{r['id']}_out.pdbqt").write_text("x", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    monkeypatch.setattr(engine.admet, "run", m1)
    monkeypatch.setattr(engine.build3d, "run", m2)
    monkeypatch.setattr(engine.meeko, "run", m3)
    monkeypatch.setattr(engine.docking_cpu, "run", m4)

    v = engine.validate(tmp_path, {"docking_mode": "cpu"})
    r = engine.run(tmp_path, {"docking_mode": "cpu"})

    assert v["config_hash"] == r["status"]["config_hash"]
    snap = r["status"]["config_snapshot"]
    assert "resolved_receptor_path" not in snap
    assert "resolved_vina_cpu_path" not in snap.get("tools", {})


def test_receptor_path_normalization_no_duplication(tmp_path):
    project_dir = tmp_path / "projects" / "example_project"
    receptor_file = project_dir / "receptors" / "target_prepared.pdbqt"
    receptor_file.parent.mkdir(parents=True, exist_ok=True)
    receptor_file.write_text("REMARK receptor\n", encoding="utf-8")

    resolved = engine.normalize_path(
        project_dir_abs=project_dir.resolve(),
        platform_root_abs=Path.cwd().resolve(),
        user_path="receptors/target_prepared.pdbqt",
        mode="receptor",
    )

    assert resolved == receptor_file.resolve()
    assert "projects/example_project/projects/example_project" not in resolved.as_posix()


def test_admet_status_normalization_accepts_legacy_values(tmp_path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\n", encoding="utf-8")
    (tmp_path / "state").mkdir(parents=True)
    (tmp_path / "state" / "manifest.csv").write_text(
        "id,smiles,inchikey,admet_status,admet_reason,sdf_status,sdf_path,sdf_reason,pdbqt_status,pdbqt_path,pdbqt_reason,vina_status,vina_score,vina_pose,vina_reason,config_hash,receptor_sha1,tools_rdkit,tools_meeko,tools_vina,created_at,updated_at\n"
        "lig,CCO,,PASSED,All rules satisfied,,,,,,,,,,,,,,,,,\n",
        encoding="utf-8",
    )

    summary = engine._build_result_summary(engine._project_paths(tmp_path))
    assert engine.is_admet_pass("PASSED") is True
    assert summary["admet_pass"] == 1


def test_funnel_counts_and_dropoffs(tmp_path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\n" + "".join(f"lig{i},CCO\n" for i in range(1, 52)), encoding="utf-8")
    (tmp_path / "state").mkdir(parents=True)

    rows = [
        "id,smiles,inchikey,admet_status,admet_reason,sdf_status,sdf_path,sdf_reason,pdbqt_status,pdbqt_path,pdbqt_reason,vina_status,vina_score,vina_pose,vina_reason,config_hash,receptor_sha1,tools_rdkit,tools_meeko,tools_vina,created_at,updated_at"
    ]
    for i in range(1, 39):
        pdbqt_status = "FAILED" if i in (37, 38) else "DONE"
        vina_status = "" if pdbqt_status == "FAILED" else "DONE"
        rows.append(
            f"lig{i},CCO,,PASS,,DONE,,,"
            f"{pdbqt_status},,,{vina_status},,,,,,,,,,,"
        )
    for i in range(39, 52):
        rows.append(f"lig{i},CCO,,FAIL,,,,,,,,,,,,,,,,,,")

    (tmp_path / "state" / "manifest.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")
    summary = engine._build_result_summary(engine._project_paths(tmp_path))

    assert summary["admet_pass"] == 38
    assert summary["admet_fail"] == 13
    assert summary["pdbqt_failed"] == 2
    assert summary["pdbqt_done"] == 36
    assert summary["vina_done"] == 36
    assert summary["vina_failed"] == 0
    assert summary["dropped_after_sdf"] == summary["sdf_done"] - summary["pdbqt_done"]


def test_funnel_counts_handle_missing_columns_and_casing(tmp_path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\n", encoding="utf-8")
    (tmp_path / "state").mkdir(parents=True)

    (tmp_path / "state" / "manifest.csv").write_text(
        "id,smiles,admet_status\n"
        "lig,CCO,  passed  \n",
        encoding="utf-8",
    )
    summary = engine._build_result_summary(engine._project_paths(tmp_path))
    assert summary["admet_pass"] == 1
    assert summary["sdf_done"] == 0
    assert summary["pdbqt_done"] == 0
    assert summary["vina_done"] == 0

    (tmp_path / "state" / "manifest.csv").write_text(
        "id,smiles,admet_status,sdf_status,pdbqt_status,vina_status\n"
        "lig,CCO,PASS, done , FAILED , ok \n",
        encoding="utf-8",
    )
    summary2 = engine._build_result_summary(engine._project_paths(tmp_path))
    assert summary2["sdf_done"] == 1
    assert summary2["pdbqt_failed"] == 1
    assert summary2["vina_done"] == 1


def test_idempotent_second_run_skips_modules(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)

    counts = {"m1": 0, "m2": 0, "m3": 0, "m4": 0}

    def m1(*a, **k):
        counts["m1"] += 1
        rows = [
            {"id": "lig", "smiles": "CCO", "admet_status": "PASS"},
            {"id": "lig2", "smiles": "CCC", "admet_status": "PASS"},
        ]
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m2(*a, **k):
        counts["m2"] += 1
        rows = [
            {"id": "lig", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "sdf_path": str((tmp_path / "3D_Structures" / "lig.sdf").resolve())},
            {"id": "lig2", "smiles": "CCC", "admet_status": "PASS", "sdf_status": "DONE", "sdf_path": str((tmp_path / "3D_Structures" / "lig2.sdf").resolve())},
        ]
        (tmp_path / "3D_Structures").mkdir(exist_ok=True)
        for r in rows:
            Path(r["sdf_path"]).write_text("ok", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m3(*a, **k):
        counts["m3"] += 1
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "prepared_ligands").mkdir(exist_ok=True)
        for r in rows:
            p = (tmp_path / "prepared_ligands" / f"{r['id']}.pdbqt").resolve()
            p.write_text("ATOM\nTORSDOF", encoding="utf-8")
            r["pdbqt_status"] = "DONE"
            r["pdbqt_path"] = str(p)
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m4(*a, **k):
        counts["m4"] += 1
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "results").mkdir(exist_ok=True)
        for r in rows:
            p = (tmp_path / "results" / f"{r['id']}_out.pdbqt").resolve()
            p.write_text("REMARK VINA RESULT: -7.0", encoding="utf-8")
            r["vina_status"] = "DONE"
            r["vina_pose"] = str(p)
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    monkeypatch.setattr(engine.admet, "run", m1)
    monkeypatch.setattr(engine.build3d, "run", m2)
    monkeypatch.setattr(engine.meeko, "run", m3)
    monkeypatch.setattr(engine.docking_cpu, "run", m4)

    res1 = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res1["exit_code"] == 0
    counts_before = dict(counts)
    res2 = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res2["exit_code"] == 0
    assert counts == counts_before


def test_deleted_pdbqt_triggers_module3_and_module4_only(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)

    calls = []

    def m1(*a, **k):
        calls.append("m1")
        rows = [
            {"id": "lig", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "sdf_path": str((tmp_path / "3D_Structures" / "lig.sdf").resolve())},
            {"id": "lig2", "smiles": "CCC", "admet_status": "PASS", "sdf_status": "DONE", "sdf_path": str((tmp_path / "3D_Structures" / "lig2.sdf").resolve())},
        ]
        (tmp_path / "3D_Structures").mkdir(exist_ok=True)
        Path(rows[0]["sdf_path"]).write_text("ok", encoding="utf-8")
        Path(rows[1]["sdf_path"]).write_text("ok", encoding="utf-8")
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m2(*a, **k):
        calls.append("m2")
        return _ok_result()

    def m3(*a, **k):
        calls.append("m3")
        ids = sorted(k.get("only_ids") or [])
        assert "lig" in ids
        rows = engine.read_manifest((tmp_path / "state" / "manifest.csv"))
        (tmp_path / "prepared_ligands").mkdir(exist_ok=True)
        p = (tmp_path / "prepared_ligands" / "lig.pdbqt").resolve()
        p.write_text("ATOM\nTORSDOF", encoding="utf-8")
        rows[0]["pdbqt_status"] = "DONE"
        rows[0]["pdbqt_path"] = str(p)
        engine.write_manifest((tmp_path / "state" / "manifest.csv"), rows)
        return _ok_result()

    def m4(*a, **k):
        calls.append("m4")
        return _ok_result()

    monkeypatch.setattr(engine.admet, "run", m1)
    monkeypatch.setattr(engine.build3d, "run", m2)
    monkeypatch.setattr(engine.meeko, "run", m3)
    monkeypatch.setattr(engine.docking_cpu, "run", m4)

    assert engine.run(tmp_path, {"docking_mode": "cpu"})["exit_code"] == 0
    Path((tmp_path / "prepared_ligands" / "lig.pdbqt")).unlink()
    calls.clear()
    assert engine.run(tmp_path, {"docking_mode": "cpu"})["exit_code"] == 0
    assert calls == ["m3", "m4"]


def test_status_missing_run_status(tmp_path):
    res = engine.status(tmp_path)
    assert res["ok"] is False
    assert res["exit_code"] == 1


def test_plan_returns_stats(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    res = engine.plan(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is True
    assert "plan" in res and "module2_todo" in res["plan"]


def test_validate_project_reports_missing_pose(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    engine.write_manifest(
        (tmp_path / "state" / "manifest.csv"),
        [{"id": "lig", "smiles": "CCO", "admet_status": "PASS", "sdf_status": "DONE", "pdbqt_status": "DONE", "vina_status": "DONE"}],
    )
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    res = engine.validate_project(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is False
    assert res["validation"]["artifact_errors"]

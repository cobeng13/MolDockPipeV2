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


def test_run_overwrite_archive_and_schema(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result())

    first = engine.run(tmp_path, {"docking_mode": "cpu"})
    second = engine.run(tmp_path, {"docking_mode": "cpu"})

    assert first["exit_code"] == 0
    assert second["exit_code"] == 0
    assert first["status"]["run_id"] != second["status"]["run_id"]
    assert second["status"]["schema_version"] == "2.0"
    assert (tmp_path / "state" / "runs" / first["status"]["run_id"] / "run_status.json").exists()


def test_progress_and_module_timestamps(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result())

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
        "lig,CCO,,PASS,,PASS,,,PASS,,,FAILED,,,No pose,,,,,,,\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result(2))

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


def test_config_snapshot_raw_and_hash_stable(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_validate_contract", _fake_contract)
    monkeypatch.setattr(engine, "_write_preflight_log", lambda *a, **k: None)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result())

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

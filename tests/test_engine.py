import json
from pathlib import Path

from moldockpipe import engine


def _setup_project(tmp_path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\nlig2,CCC\n", encoding="utf-8")
    (tmp_path / "receptors").mkdir(parents=True)
    (tmp_path / "receptors" / "target_prepared.pdbqt").write_text("REMARK receptor\n", encoding="utf-8")


def _fake_checks(project_dir, config, warnings):
    config.setdefault("tools", {})["resolved_vina_cpu_path"] = str(project_dir / "vina.exe")
    config.setdefault("tools", {})["resolved_vina_gpu_path"] = str(project_dir / "vina-gpu.exe")
    config["resolved_docking"] = {
        "center_x": 0.0,
        "center_y": 0.0,
        "center_z": 0.0,
        "size_x": 20.0,
        "size_y": 20.0,
        "size_z": 20.0,
        "exhaustiveness": 8,
        "num_modes": 9,
        "energy_range": 3.0,
    }
    return {"python": "3.11.9", "rdkit": "2025.3.6", "meeko": "0.6.1", "pandas": "2.2.3"}


def _ok_result():
    class Ok:
        returncode = 0
        stdout_log = "stdout.log"
        stderr_log = "stderr.log"

        @property
        def ok(self):
            return True

    return Ok()


def test_engine_run_then_status_and_progress(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result())

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is True

    stat = engine.status(tmp_path)
    rs = stat["run_status"]
    assert rs["phase"] == "completed"
    assert rs["progress"]["percent"] == 100
    assert rs["run_id"]
    assert rs["config_hash"]
    assert rs["result_summary"]["input_rows"] == 2

    percents = [
        int(h["module"].split("module")[-1][0]) * 25 if False else None
        for h in rs["history"]
    ]
    assert len(rs["history"]) == 4
    assert all("started_at" in h and "ended_at" in h and "duration_seconds" in h and "run_id" in h for h in rs["history"])


def test_new_run_overwrites_and_archives_prior_status(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result())

    first = engine.run(tmp_path, {"docking_mode": "cpu"})["status"]["run_id"]
    second = engine.run(tmp_path, {"docking_mode": "cpu"})["status"]["run_id"]

    assert first != second
    current = json.loads((tmp_path / "state" / "run_status.json").read_text(encoding="utf-8"))
    assert current["run_id"] == second
    assert (tmp_path / "state" / "runs" / first / "run_status.json").exists()


def test_completed_with_errors_when_module4_returns_2(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    class Warn:
        returncode = 2
        stdout_log = "stdout.log"
        stderr_log = "stderr.log"

        @property
        def ok(self):
            return False

    # seed manifest so summary can show failed docking
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    (tmp_path / "state" / "manifest.csv").write_text(
        "id,smiles,inchikey,admet_status,admet_reason,sdf_status,sdf_path,sdf_reason,pdbqt_status,pdbqt_path,pdbqt_reason,vina_status,vina_score,vina_pose,vina_reason,config_hash,receptor_sha1,tools_rdkit,tools_meeko,tools_vina,created_at,updated_at\n"
        "lig,CCO,,PASS,,PASS,,,PASS,,,FAILED,,,No pose,,,,,,,\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: Warn())

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is True
    assert res["status"]["phase"] == "completed_with_errors"
    assert res["status"]["result_summary"]["docked_failed"] > 0


def test_config_hash_stable_between_validate_and_run(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: _ok_result())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: _ok_result())

    v = engine.validate(tmp_path, {"docking_mode": "cpu"})
    r = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert v["config_hash"] == r["status"]["config_hash"]


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

import json

from moldockpipe import engine


def _setup_project(tmp_path):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\n", encoding="utf-8")
    (tmp_path / "receptors").mkdir(parents=True)
    (tmp_path / "receptors" / "target_prepared.pdbqt").write_text("REMARK receptor\n", encoding="utf-8")


def _fake_checks(project_dir, config, warnings):
    config["resolved_vina_cpu_path"] = str(project_dir / "vina.exe")
    config["resolved_vina_gpu_path"] = str(project_dir / "vina-gpu.exe")
    return {"python": "3.11.9", "rdkit": "2025.3.6", "meeko": "0.6.1", "pandas": "2.2.3"}


def test_engine_run_then_status(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    class Ok:
        returncode = 0
        stdout_log = "stdout.log"
        stderr_log = "stderr.log"

        @property
        def ok(self):
            return True

    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: Ok())

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is True
    stat = engine.status(tmp_path)
    assert stat["run_status"]["phase"] == "completed"
    assert stat["run_status"]["runtime"]["python_executable"]
    assert stat["run_status"]["config_hash"]


def test_engine_resume_skips_completed(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    (tmp_path / "state").mkdir(parents=True)
    (tmp_path / "state" / "run_status.json").write_text(
        json.dumps(
            {
                "phase": "failed",
                "completed_modules": ["module1_admet"],
                "failed_module": "module2_build3d",
                "config_snapshot": {"docking_mode": "cpu"},
                "started_at": None,
                "updated_at": None,
                "history": [],
            }
        ),
        encoding="utf-8",
    )

    calls = {"admet": 0}

    class Ok:
        returncode = 0
        stdout_log = "stdout.log"
        stderr_log = "stderr.log"

        @property
        def ok(self):
            return True

    def admet_run(p, l):
        calls["admet"] += 1
        return Ok()

    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)
    monkeypatch.setattr(engine.admet, "run", admet_run)
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l, **k: Ok())

    res = engine.resume(tmp_path)
    assert res["ok"] is True
    assert calls["admet"] == 0


def test_engine_preflight_missing_rdkit_fails_before_modules(tmp_path, monkeypatch):
    _setup_project(tmp_path)

    monkeypatch.setattr(engine.importlib.util, "find_spec", lambda name: None if name == "rdkit" else object())

    called = {"admet": 0}

    def admet_run(p, l):
        called["admet"] += 1

    monkeypatch.setattr(engine.admet, "run", admet_run)

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is False
    assert res["failed_module"] == "preflight"
    assert "RDKit is required for Module 2" in res["error"]
    assert called["admet"] == 0
    assert (tmp_path / "logs" / "preflight.log").exists()


def test_config_hash_deterministic(tmp_path, monkeypatch):
    _setup_project(tmp_path)
    (tmp_path / "config").mkdir(parents=True)
    (tmp_path / "config" / "run.yml").write_text("docking_mode: cpu\n", encoding="utf-8")
    monkeypatch.setattr(engine, "_run_preflight_checks", _fake_checks)

    v1 = engine.validate(tmp_path, {"docking_mode": "cpu"})
    v2 = engine.validate(tmp_path, {"docking_mode": "cpu"})
    v3 = engine.validate(tmp_path, {"docking_mode": "gpu"})

    assert v1["config_hash"] == v2["config_hash"]
    assert v1["config_hash"] != v3["config_hash"]

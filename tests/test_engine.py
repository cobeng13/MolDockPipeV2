from moldockpipe import engine


def test_engine_run_then_status(tmp_path, monkeypatch):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\n", encoding="utf-8")

    class Ok:
        returncode = 0
        stdout_log = "stdout.log"
        stderr_log = "stderr.log"

        @property
        def ok(self):
            return True

    monkeypatch.setattr(engine, "_run_preflight_checks", lambda p, c: None)
    monkeypatch.setattr(engine.admet, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l: Ok())

    res = engine.run(tmp_path, {"docking_mode": "cpu"})
    assert res["ok"] is True
    stat = engine.status(tmp_path)
    assert stat["run_status"]["phase"] == "completed"
    assert stat["run_status"]["runtime"]["python_executable"]


def test_engine_resume_skips_completed(tmp_path, monkeypatch):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\n", encoding="utf-8")
    (tmp_path / "state").mkdir(parents=True)
    (tmp_path / "state" / "run_status.json").write_text(
        '{"phase":"failed","completed_modules":["module1_admet"],"failed_module":"module2_build3d","config":{"docking_mode":"cpu"},"started_at":null,"updated_at":null,"history":[]}',
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

    monkeypatch.setattr(engine, "_run_preflight_checks", lambda p, c: None)
    monkeypatch.setattr(engine.admet, "run", admet_run)
    monkeypatch.setattr(engine.build3d, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.meeko, "run", lambda p, l: Ok())
    monkeypatch.setattr(engine.docking_cpu, "run", lambda p, l: Ok())

    res = engine.resume(tmp_path)
    assert res["ok"] is True
    assert calls["admet"] == 0


def test_engine_preflight_missing_rdkit_fails_before_modules(tmp_path, monkeypatch):
    (tmp_path / "input").mkdir(parents=True)
    (tmp_path / "input" / "input.csv").write_text("id,smiles\nlig,CCO\n", encoding="utf-8")

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
    assert (tmp_path / "logs" / "engine" / "preflight.log").exists()

from click.testing import CliRunner

from moldockpipe.cli import app, main


def test_cli_status(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.engine.status", lambda p: {"ok": True, "exit_code": 0, "status": {"phase": "completed", "project_dir": str(p)}})
    result = runner.invoke(app, ["status", "./demo", "--json"])
    assert result.exit_code == 0
    assert '"phase": "completed"' in result.output


def test_cli_help_works():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "MolDockPipe orchestrator CLI" in result.output


def test_cli_validate_exit_code(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.engine.validate_project", lambda *a, **k: {"ok": False, "exit_code": 1, "validation": {"summary": {"errors_found": 1}}})
    result = runner.invoke(app, ["validate", "./demo", "--docking-mode", "cpu", "--json"])
    assert result.exit_code == 1
    assert '"errors_found": 1' in result.output


def test_main_callable_exists():
    assert callable(main)


def test_cli_run_no_ui_uses_engine_result(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.engine.run", lambda *a, **k: {"ok": True, "exit_code": 0, "status": {"status": "completed"}})
    result = runner.invoke(app, ["run", "./demo", "--docking-mode", "cpu", "--no-ui"])
    assert result.exit_code == 0
    assert '"ok": true' in result.output


def test_cli_run_auto_disables_ui_when_not_tty(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.cli._ui_enabled", lambda no_ui: False)
    monkeypatch.setattr("moldockpipe.engine.run", lambda *a, **k: {"exit_code": 3, "error": "validation"})
    result = runner.invoke(app, ["run", "./demo", "--docking-mode", "cpu"])
    assert result.exit_code == 3
    assert '"error": "validation"' in result.output


def test_cli_plan_json(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.engine.plan", lambda *a, **k: {"ok": True, "exit_code": 0, "plan": {"module2_todo": 0}})
    result = runner.invoke(app, ["plan", "./demo", "--json"])
    assert result.exit_code == 0
    assert '"module2_todo": 0' in result.output

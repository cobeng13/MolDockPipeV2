from click.testing import CliRunner

from moldockpipe.cli import app, main


def test_cli_status(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.engine.status", lambda p: {"ok": True, "project_dir": str(p)})
    result = runner.invoke(app, ["status", "./demo"])
    assert result.exit_code == 0
    assert '"ok": true' in result.output


def test_cli_help_works():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "MolDockPipe orchestrator CLI" in result.output


def test_cli_validate(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr("moldockpipe.engine.validate", lambda p, c: {"ok": True, "warnings": []})
    result = runner.invoke(app, ["validate", "./demo", "--docking-mode", "cpu"])
    assert result.exit_code == 0
    assert '"ok": true' in result.output


def test_main_callable_exists():
    assert callable(main)

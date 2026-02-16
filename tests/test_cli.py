from click.testing import CliRunner

from moldockpipe.cli import app


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

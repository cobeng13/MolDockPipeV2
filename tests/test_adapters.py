from pathlib import Path
from types import SimpleNamespace

from moldockpipe.adapters import admet


def test_adapter_invokes_subprocess_and_writes_logs(tmp_path, monkeypatch):
    called = {}

    def fake_run(cmd, cwd, text, capture_output, check):
        called["cmd"] = cmd
        called["cwd"] = cwd
        return SimpleNamespace(returncode=0, stdout="ok out", stderr="ok err")

    monkeypatch.setattr("moldockpipe.adapters.common.subprocess.run", fake_run)
    result = admet.run(tmp_path, tmp_path / "logs")

    assert result.ok
    assert Path(result.stdout_log).read_text() == "ok out"
    assert Path(result.stderr_log).read_text() == "ok err"
    assert "Module 1.py" in " ".join(called["cmd"])

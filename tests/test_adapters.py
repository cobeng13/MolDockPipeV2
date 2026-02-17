import sys
from pathlib import Path
from types import SimpleNamespace

from moldockpipe.adapters import admet
from moldockpipe.adapters.common import run_script


def test_adapter_invokes_subprocess_and_writes_logs(tmp_path, monkeypatch):
    called = {}

    def fake_run(cmd, cwd, env, text, encoding, errors, capture_output, check):
        called["cmd"] = cmd
        called["cwd"] = cwd
        called["env"] = env
        called["encoding"] = encoding
        called["errors"] = errors
        return SimpleNamespace(returncode=0, stdout="ok out", stderr="ok err")

    monkeypatch.setattr("moldockpipe.adapters.common.subprocess.run", fake_run)
    result = admet.run(tmp_path, tmp_path / "logs")

    assert result.ok
    assert Path(result.stdout_log).read_text(encoding="utf-8") == "ok out"
    assert Path(result.stderr_log).read_text(encoding="utf-8") == "ok err"
    assert "Module 1.py" in " ".join(called["cmd"])
    assert called["cmd"][0] == sys.executable
    assert called["env"]["PYTHONUTF8"] == "1"
    assert called["env"]["PYTHONIOENCODING"] == "utf-8"
    assert called["encoding"] == "utf-8"
    assert called["errors"] == "replace"


def test_subprocess_runner_handles_emoji_output(tmp_path, monkeypatch):
    fixture_script = Path(__file__).parent / "fixtures" / "print_emoji.py"

    monkeypatch.setattr("moldockpipe.adapters.common.REPO_ROOT", fixture_script.parent)
    monkeypatch.setattr("moldockpipe.adapters.common.sys.executable", sys.executable)

    result = run_script(
        module="emoji_module",
        script_name=fixture_script.name,
        project_dir=tmp_path,
        logs_dir=tmp_path / "logs" / "engine",
    )

    assert result.ok
    log_text = Path(result.stdout_log).read_text(encoding="utf-8")
    assert "✅" in log_text

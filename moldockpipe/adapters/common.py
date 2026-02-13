from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class AdapterResult:
    module: str
    returncode: int
    command: list[str]
    stdout_log: str
    stderr_log: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def run_script(module: str, script_name: str, project_dir: Path, logs_dir: Path) -> AdapterResult:
    script_path = REPO_ROOT / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Cannot find canonical script: {script_path}")

    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_log = logs_dir / f"{module}.stdout.log"
    stderr_log = logs_dir / f"{module}.stderr.log"

    cmd = [sys.executable, str(script_path)]
    proc = subprocess.run(
        cmd,
        cwd=project_dir,
        text=True,
        capture_output=True,
        check=False,
    )
    stdout_log.write_text(proc.stdout or "", encoding="utf-8")
    stderr_log.write_text(proc.stderr or "", encoding="utf-8")

    return AdapterResult(
        module=module,
        returncode=proc.returncode,
        command=cmd,
        stdout_log=str(stdout_log),
        stderr_log=str(stderr_log),
    )

from __future__ import annotations

import os
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


def run_script(
    module: str,
    script_name: str,
    project_dir: Path,
    logs_dir: Path,
    args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
) -> AdapterResult:
    script_path = REPO_ROOT / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Cannot find canonical script: {script_path}")

    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_log = logs_dir / f"{module}.stdout.log"
    stderr_log = logs_dir / f"{module}.stderr.log"

    # Force UTF-8 for child Python processes to avoid Windows cp1252/charmap
    # failures on emoji/non-ASCII writes in redirected stdout/stderr (PEP 540/597).
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("PYTHONLEGACYWINDOWSSTDIO", "0")
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})

    cmd = [sys.executable, str(script_path), *(args or [])]
    proc = subprocess.run(
        cmd,
        cwd=project_dir,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    stdout_log.write_text(proc.stdout or "", encoding="utf-8", errors="replace")
    stderr_log.write_text(proc.stderr or "", encoding="utf-8", errors="replace")

    return AdapterResult(
        module=module,
        returncode=proc.returncode,
        command=cmd,
        stdout_log=str(stdout_log),
        stderr_log=str(stderr_log),
    )


def only_ids_env(project_dir: Path, module: str, only_ids: set[str] | None) -> dict[str, str] | None:
    if not only_ids:
        return None
    work = (project_dir / "state" / f"work_ids_{module}.txt").resolve()
    work.parent.mkdir(parents=True, exist_ok=True)
    work.write_text("\n".join(sorted(only_ids)) + "\n", encoding="utf-8")
    return {"MOLDOCK_ONLY_IDS_FILE": str(work)}

from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, run_script


def run(project_dir: Path, logs_dir: Path, vina_path: str | None = None) -> AdapterResult:
    args = ["--vina", vina_path] if vina_path else []
    env = {"MOLDOCK_VINA_CPU_PATH": vina_path} if vina_path else None
    return run_script("module4a_cpu", "Module 4a (CPU).py", project_dir, logs_dir, args=args, extra_env=env)

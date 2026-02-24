from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, only_ids_env, run_script


def run(project_dir: Path, logs_dir: Path, only_ids: set[str] | None = None) -> AdapterResult:
    env = only_ids_env(project_dir, "module1_admet", only_ids)
    return run_script("module1_admet", "Module 1.py", project_dir, logs_dir, extra_env=env)

from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, only_ids_env, run_script


def run(project_dir: Path, logs_dir: Path, only_ids: set[str] | None = None) -> AdapterResult:
    env = only_ids_env(project_dir, "module3_meeko", only_ids)
    return run_script("module3_meeko", "Module 3 (Parallel).py", project_dir, logs_dir, extra_env=env)

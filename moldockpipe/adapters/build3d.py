from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, run_script


def run(project_dir: Path, logs_dir: Path, only_ids_path: Path | None = None) -> AdapterResult:
    env = {"MOLDOCK_ONLY_IDS_FILE": str(only_ids_path)} if only_ids_path else None
    return run_script("module2_build3d", "Module 2.py", project_dir, logs_dir, extra_env=env)

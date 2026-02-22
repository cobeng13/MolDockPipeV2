from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, run_script


def run(project_dir: Path, logs_dir: Path) -> AdapterResult:
    return run_script("module4b_gpu", "Module 4b (GPU)v3.py", project_dir, logs_dir)

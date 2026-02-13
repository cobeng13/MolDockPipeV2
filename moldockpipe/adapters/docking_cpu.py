from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, run_script


def run(project_dir: Path, logs_dir: Path) -> AdapterResult:
    return run_script("module4a_cpu", "Module 4a (CPU).py", project_dir, logs_dir)

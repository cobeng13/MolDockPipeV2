from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, run_script


def run(project_dir: Path, logs_dir: Path) -> AdapterResult:
    return run_script("module3_meeko", "Module 3 (Parallel).py", project_dir, logs_dir)

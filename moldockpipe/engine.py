from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from moldockpipe.adapters import admet, build3d, docking_cpu, docking_gpu, meeko
from moldockpipe.adapters.common import REPO_ROOT
from moldockpipe.state import read_manifest, read_run_status, update_run_status, write_manifest

MODULES: list[str] = [
    "module1_admet",
    "module2_build3d",
    "module3_meeko",
    "module4_docking",
]

CPU_VINA_CANDIDATES = ["vina", "vina.exe", "vina_1.2.7_win.exe", "vina_1.2.5_win.exe"]
GPU_VINA_CANDIDATES = ["Vina-GPU+.exe", "Vina-GPU+_K.exe", "Vina-GPU.exe", "vina-gpu.exe", "vina-gpu"]


class PreflightError(RuntimeError):
    pass


def _project_paths(project_dir: Path) -> dict[str, Path]:
    return {
        "project": project_dir,
        "input_csv": project_dir / "input" / "input.csv",
        "state_dir": project_dir / "state",
        "status_json": project_dir / "state" / "run_status.json",
        "manifest_csv": project_dir / "state" / "manifest.csv",
        "logs_dir": project_dir / "logs" / "engine",
        "preflight_log": project_dir / "logs" / "engine" / "preflight.log",
    }


def _runtime_info() -> dict:
    return {
        "python_executable": sys.executable,
        "python_version": sys.version,
        "cwd": str(Path.cwd()),
    }


def _write_preflight_log(paths: dict[str, Path]) -> None:
    lines = [
        f"python_executable={sys.executable}",
        f"python_version={sys.version}",
        f"cwd={Path.cwd()}",
        f"project_dir={paths['project']}",
        f"PATH={os.environ.get('PATH', '')}",
        "subprocess_cwd_rule=project_dir",
    ]
    paths["preflight_log"].parent.mkdir(parents=True, exist_ok=True)
    paths["preflight_log"].write_text("\n".join(lines) + "\n", encoding="utf-8", errors="replace")


def _binary_exists(candidates: list[str], project_dir: Path) -> bool:
    for name in candidates:
        if (REPO_ROOT / name).exists() or (project_dir / name).exists():
            return True
    return False


def _run_preflight_checks(project_dir: Path, config: dict) -> None:
    if importlib.util.find_spec("rdkit") is None:
        raise PreflightError(
            "RDKit is required for Module 2. Install RDKit in this environment (recommended: conda-forge)."
        )
    if importlib.util.find_spec("meeko") is None:
        raise PreflightError("Meeko is required for Module 3. Install it in this environment: pip install meeko")

    mode = (config.get("docking_mode") or "cpu").lower()
    if mode == "gpu":
        if not _binary_exists(GPU_VINA_CANDIDATES, project_dir):
            raise PreflightError(
                "GPU docking selected but Vina-GPU binary was not found in repository root or project directory."
            )
    else:
        if not _binary_exists(CPU_VINA_CANDIDATES, project_dir):
            raise PreflightError(
                "CPU docking selected but Vina executable was not found in repository root or project directory."
            )


def _preflight(project_dir: Path, config: dict) -> dict[str, Path]:
    paths = _project_paths(project_dir)
    if not project_dir.exists():
        raise PreflightError(f"project_dir does not exist: {project_dir}")
    if not paths["input_csv"].exists():
        raise PreflightError(f"Missing required input file: {paths['input_csv']}")

    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    paths["logs_dir"].mkdir(parents=True, exist_ok=True)
    _write_preflight_log(paths)

    if not paths["manifest_csv"].exists():
        write_manifest(paths["manifest_csv"], [])

    _run_preflight_checks(project_dir, config)
    return paths


def _history_append(status_path: Path, entry: dict) -> None:
    status = read_run_status(status_path)
    history = list(status.get("history", []))
    history.append(entry)
    update_run_status(status_path, history=history)


def _run_docking(project_dir: Path, logs_dir: Path, config: dict):
    mode = (config.get("docking_mode") or "cpu").lower()
    if mode == "gpu":
        return docking_gpu.run(project_dir, logs_dir)
    return docking_cpu.run(project_dir, logs_dir)


def _preflight_failure(project_dir: Path, config: dict, error: Exception) -> dict:
    paths = _project_paths(project_dir)
    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    status = update_run_status(
        paths["status_json"],
        phase="failed",
        failed_module="preflight",
        completed_modules=[],
        config=config,
        error=str(error),
        runtime=_runtime_info(),
    )
    return {
        "ok": False,
        "failed_module": "preflight",
        "error": str(error),
        "results": [],
        "status": status,
        "manifest_rows": len(read_manifest(paths["manifest_csv"])),
    }


def _execute(project_dir: Path, config: dict, resume_mode: bool) -> dict:
    try:
        paths = _preflight(project_dir, config)
    except PreflightError as exc:
        return _preflight_failure(project_dir, config, exc)

    status_path = paths["status_json"]
    current = read_run_status(status_path)
    completed = set(current.get("completed_modules", [])) if resume_mode else set()

    update_run_status(
        status_path,
        phase="running",
        failed_module=None,
        completed_modules=sorted(completed),
        config=config,
        runtime=_runtime_info(),
    )

    results = []
    for module_name in MODULES:
        if module_name in completed:
            continue

        if module_name == "module1_admet":
            result = admet.run(project_dir, paths["logs_dir"])
        elif module_name == "module2_build3d":
            result = build3d.run(project_dir, paths["logs_dir"])
        elif module_name == "module3_meeko":
            result = meeko.run(project_dir, paths["logs_dir"])
        else:
            result = _run_docking(project_dir, paths["logs_dir"], config)

        record = {
            "module": module_name,
            "returncode": result.returncode,
            "stdout_log": result.stdout_log,
            "stderr_log": result.stderr_log,
            "ok": result.ok,
            "python_executable": sys.executable,
        }
        results.append(record)
        _history_append(status_path, record)

        if not result.ok:
            update_run_status(
                status_path,
                phase="failed",
                failed_module=module_name,
                completed_modules=sorted(completed),
                runtime=_runtime_info(),
            )
            return {
                "ok": False,
                "failed_module": module_name,
                "results": results,
                "status": read_run_status(status_path),
                "manifest_rows": len(read_manifest(paths["manifest_csv"])),
            }

        completed.add(module_name)
        update_run_status(status_path, completed_modules=sorted(completed), runtime=_runtime_info())

    update_run_status(
        status_path,
        phase="completed",
        failed_module=None,
        completed_modules=sorted(completed),
        runtime=_runtime_info(),
    )
    return {
        "ok": True,
        "results": results,
        "status": read_run_status(status_path),
        "manifest_rows": len(read_manifest(paths["manifest_csv"])),
    }


def run(project_dir: Path, config: dict) -> dict:
    return _execute(project_dir=project_dir, config=config, resume_mode=False)


def resume(project_dir: Path) -> dict:
    current = read_run_status(project_dir / "state" / "run_status.json")
    config = current.get("config", {})
    return _execute(project_dir=project_dir, config=config, resume_mode=True)


def status(project_dir: Path) -> dict:
    paths = _project_paths(project_dir)
    return {
        "run_status": read_run_status(paths["status_json"]),
        "manifest_rows": len(read_manifest(paths["manifest_csv"])),
        "project_dir": str(project_dir),
    }


def export_report(project_dir: Path) -> dict:
    rows = read_manifest(project_dir / "state" / "manifest.csv")
    out = project_dir / "results" / "engine_report.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    write_manifest(out, rows)
    return {"rows": len(rows), "report": str(out)}

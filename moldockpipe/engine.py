from __future__ import annotations

from pathlib import Path

from moldockpipe.adapters import admet, build3d, docking_cpu, docking_gpu, meeko
from moldockpipe.state import read_manifest, read_run_status, update_run_status, write_manifest

MODULES: list[str] = [
    "module1_admet",
    "module2_build3d",
    "module3_meeko",
    "module4_docking",
]


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
    }


def _preflight(project_dir: Path) -> dict[str, Path]:
    paths = _project_paths(project_dir)
    if not project_dir.exists():
        raise PreflightError(f"project_dir does not exist: {project_dir}")
    if not paths["input_csv"].exists():
        raise PreflightError(f"Missing required input file: {paths['input_csv']}")
    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    paths["logs_dir"].mkdir(parents=True, exist_ok=True)
    if not paths["manifest_csv"].exists():
        write_manifest(paths["manifest_csv"], [])
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


def _execute(project_dir: Path, config: dict, resume_mode: bool) -> dict:
    paths = _preflight(project_dir)
    status_path = paths["status_json"]
    current = read_run_status(status_path)
    completed = set(current.get("completed_modules", [])) if resume_mode else set()

    update_run_status(
        status_path,
        phase="running",
        failed_module=None,
        completed_modules=sorted(completed),
        config=config,
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
        }
        results.append(record)
        _history_append(status_path, record)

        if not result.ok:
            update_run_status(
                status_path,
                phase="failed",
                failed_module=module_name,
                completed_modules=sorted(completed),
            )
            return {
                "ok": False,
                "failed_module": module_name,
                "results": results,
                "status": read_run_status(status_path),
                "manifest_rows": len(read_manifest(paths["manifest_csv"])),
            }

        completed.add(module_name)
        update_run_status(status_path, completed_modules=sorted(completed))

    update_run_status(status_path, phase="completed", failed_module=None, completed_modules=sorted(completed))
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

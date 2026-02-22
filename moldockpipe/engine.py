from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import shutil
import sys
from pathlib import Path

from moldockpipe.adapters import admet, build3d, docking_cpu, docking_gpu, meeko
from moldockpipe.adapters.common import REPO_ROOT
from moldockpipe.state import read_manifest, read_run_status, update_run_status, write_manifest

try:
    import yaml
except Exception:  # pragma: no cover - guarded by dependency but kept safe
    yaml = None

MODULES: list[str] = [
    "module1_admet",
    "module2_build3d",
    "module3_meeko",
    "module4_docking",
]

DEFAULT_CONFIG = {
    "docking_mode": "cpu",
    "strict_versions": False,
    "receptor_path": "receptors/target_prepared.pdbqt",
    "tools": {
        "vina_cpu_path": "tools/vina_1.2.7_win.exe",
        "vina_gpu_path": "tools/vina-gpu.exe",
    },
}

CPU_VINA_CANDIDATES = ["vina", "vina.exe", "vina_1.2.7_win.exe", "vina_1.2.5_win.exe"]
GPU_VINA_CANDIDATES = ["Vina-GPU+.exe", "Vina-GPU+_K.exe", "Vina-GPU.exe", "vina-gpu.exe", "vina-gpu"]

RECOMMENDED = {
    "python": "3.11",
    "rdkit": "2025.03.",
    "meeko": "0.6.1",
}


class PreflightError(RuntimeError):
    pass


def _deep_update(dst: dict, src: dict) -> dict:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            _deep_update(dst[key], value)
        else:
            dst[key] = value
    return dst


def _canonical_json(data: dict) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _config_hash(config: dict) -> str:
    return hashlib.sha256(_canonical_json(config).encode("utf-8")).hexdigest()


def _project_paths(project_dir: Path) -> dict[str, Path]:
    return {
        "project": project_dir,
        "input_csv": project_dir / "input" / "input.csv",
        "state_dir": project_dir / "state",
        "status_json": project_dir / "state" / "run_status.json",
        "manifest_csv": project_dir / "state" / "manifest.csv",
        "logs_dir": project_dir / "logs",
        "engine_logs_dir": project_dir / "logs" / "engine",
        "preflight_log": project_dir / "logs" / "preflight.log",
        "results_dir": project_dir / "results",
        "structures_dir": project_dir / "3D_Structures",
        "prepared_dir": project_dir / "prepared_ligands",
        "run_yml": project_dir / "config" / "run.yml",
    }


def _runtime_info() -> dict:
    return {
        "python_executable": sys.executable,
        "python_version": sys.version,
        "platform": platform.platform(),
        "cwd": str(Path.cwd()),
    }


def _import_version(module_name: str) -> str | None:
    try:
        module = importlib.import_module(module_name)
        return getattr(module, "__version__", None)
    except Exception:
        return None


def _package_version(package_name: str) -> str | None:
    try:
        return importlib.metadata.version(package_name)
    except Exception:
        return None


def _collect_versions() -> dict:
    rdkit_ver = _import_version("rdkit")
    meeko_ver = _import_version("meeko") or _package_version("meeko")
    pandas_ver = _import_version("pandas")
    return {
        "python": platform.python_version(),
        "rdkit": rdkit_ver,
        "meeko": meeko_ver,
        "pandas": pandas_ver,
    }




def _normalize_version_triplet(value: str | None) -> tuple[int, ...] | None:
    if not value:
        return None
    cleaned = str(value).strip().split("+")[0]
    parts = []
    for token in cleaned.replace("-", ".").split("."):
        if token.isdigit():
            parts.append(int(token))
        else:
            digits = "".join(ch for ch in token if ch.isdigit())
            if digits:
                parts.append(int(digits))
    return tuple(parts) if parts else None


def _rdkit_matches_recommended(value: str | None) -> bool:
    trip = _normalize_version_triplet(value)
    return bool(trip and len(trip) >= 2 and trip[0] == 2025 and trip[1] == 3)


def _meeko_matches_recommended(value: str | None) -> bool:
    trip = _normalize_version_triplet(value)
    return bool(trip and tuple(trip[:3]) == (0, 6, 1))

def _version_warnings(versions: dict) -> list[str]:
    warnings = []
    if not versions["python"].startswith(RECOMMENDED["python"]):
        warnings.append(f"Recommended Python is {RECOMMENDED['python']} (detected {versions['python']}).")
    if versions.get("rdkit") and not _rdkit_matches_recommended(versions["rdkit"]):
        warnings.append(
            f"Recommended RDKit series is 2025.03.* (detected {versions['rdkit']})."
        )
    if versions.get("meeko") and not _meeko_matches_recommended(versions["meeko"]):
        warnings.append(f"Recommended Meeko is {RECOMMENDED['meeko']} (detected {versions['meeko']}).")
    return warnings


def _resolve_tool_path(configured: str | None, project_dir: Path, candidates: list[str]) -> tuple[str | None, str | None]:
    if configured:
        p = Path(configured)
        if p.is_absolute():
            return (str(p.resolve()) if p.exists() else None), None
        project_p = (project_dir / p).resolve()
        if project_p.exists():
            return str(project_p), None
        platform_p = (REPO_ROOT / p).resolve()
        if platform_p.exists():
            return str(platform_p), None
        return None, f"Configured tool path not found: {configured}"

    for candidate in candidates:
        for base in (project_dir, REPO_ROOT):
            p = base / candidate
            if p.exists():
                return str(p.resolve()), f"Configured path missing; used fallback candidate '{candidate}'."
        found = shutil.which(candidate)
        if found:
            return found, f"Configured path missing; used PATH fallback '{candidate}'."
    return None, None


def _load_project_config(project_dir: Path, cli_config: dict | None) -> tuple[dict, list[str]]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    warnings: list[str] = []
    run_yml = project_dir / "config" / "run.yml"

    if run_yml.exists():
        if yaml is None:
            warnings.append("run.yml exists but PyYAML is unavailable; using defaults + CLI overrides.")
        else:
            data = yaml.safe_load(run_yml.read_text(encoding="utf-8")) or {}
            if isinstance(data, dict):
                _deep_update(cfg, data)
    else:
        warnings.append("config/run.yml not found; using defaults + CLI overrides.")

    if cli_config:
        _deep_update(cfg, cli_config)

    cfg["docking_mode"] = str(cfg.get("docking_mode", "cpu")).lower()
    return cfg, warnings


def _write_preflight_log(paths: dict[str, Path], config: dict, config_hash: str, versions: dict, warnings: list[str]) -> None:
    lines = [
        f"python_executable={sys.executable}",
        f"python_version={sys.version}",
        f"platform={platform.platform()}",
        f"cwd={Path.cwd()}",
        f"project_dir={paths['project']}",
        f"config_hash={config_hash}",
        f"docking_mode={config.get('docking_mode')}",
        f"resolved_receptor={config.get('resolved_receptor_path')}",
        f"resolved_vina_cpu={config.get('resolved_vina_cpu_path')}",
        f"resolved_vina_gpu={config.get('resolved_vina_gpu_path')}",
        f"rdkit_version={versions.get('rdkit')}",
        f"meeko_version={versions.get('meeko')}",
        f"pandas_version={versions.get('pandas')}",
    ]
    for warning in warnings:
        lines.append(f"warning={warning}")
    paths["preflight_log"].parent.mkdir(parents=True, exist_ok=True)
    paths["preflight_log"].write_text("\n".join(lines) + "\n", encoding="utf-8", errors="replace")


def _ensure_dirs(paths: dict[str, Path]) -> None:
    for key in ("state_dir", "logs_dir", "engine_logs_dir", "results_dir", "structures_dir", "prepared_dir"):
        paths[key].mkdir(parents=True, exist_ok=True)


def _validate_project_contract(paths: dict[str, Path], config: dict, warnings: list[str]) -> None:
    if not paths["project"].exists():
        raise PreflightError(f"project_dir does not exist: {paths['project']}")
    if not paths["input_csv"].exists():
        raise PreflightError(f"Missing required input file: {paths['input_csv']}")

    receptor_cfg = config.get("receptor_path") or "receptors/target_prepared.pdbqt"
    receptor = Path(receptor_cfg)
    if not receptor.is_absolute():
        receptor = paths["project"] / receptor
    config["resolved_receptor_path"] = str(receptor)
    if not receptor.exists():
        raise PreflightError(f"Missing receptor file: {receptor}")

    if not paths["run_yml"].exists():
        warnings.append("config/run.yml missing (allowed).")


def _run_preflight_checks(project_dir: Path, config: dict, warnings: list[str]) -> dict:
    if importlib.util.find_spec("rdkit") is None:
        raise PreflightError(
            "RDKit is required for Module 2. Install RDKit in this environment (recommended: conda-forge, 2025.03.*)."
        )
    if importlib.util.find_spec("meeko") is None:
        raise PreflightError("Meeko is required for Module 3. Install it in this environment: pip install meeko==0.6.1")

    cpu_path, cpu_warn = _resolve_tool_path(config.get("tools", {}).get("vina_cpu_path"), project_dir, CPU_VINA_CANDIDATES)
    gpu_path, gpu_warn = _resolve_tool_path(config.get("tools", {}).get("vina_gpu_path"), project_dir, GPU_VINA_CANDIDATES)
    config["resolved_vina_cpu_path"] = cpu_path
    config["resolved_vina_gpu_path"] = gpu_path

    if cpu_warn:
        warnings.append(cpu_warn)
    if gpu_warn:
        warnings.append(gpu_warn)

    mode = config.get("docking_mode", "cpu")
    if mode == "gpu" and not gpu_path:
        attempted = config.get("tools", {}).get("vina_gpu_path") or "tools/vina-gpu.exe"
        raise PreflightError(
            f"GPU docking selected but no Vina-GPU binary was found. Checked configured tools.vina_gpu_path={attempted}. "
            f"Default Option 1 location is {REPO_ROOT / 'tools'}."
        )
    if mode == "cpu" and not cpu_path:
        attempted = config.get("tools", {}).get("vina_cpu_path") or "tools/vina_1.2.7_win.exe"
        raise PreflightError(
            f"CPU docking selected but no Vina binary was found. Checked configured tools.vina_cpu_path={attempted}. "
            f"Default Option 1 location is {REPO_ROOT / 'tools'}."
        )

    versions = _collect_versions()
    warnings.extend(_version_warnings(versions))
    if config.get("strict_versions") and _version_warnings(versions):
        raise PreflightError("Strict version mode enabled and recommended toolchain versions were not met.")
    return versions


def _stamp_manifest_config_hash(paths: dict[str, Path], config_hash: str) -> None:
    rows = read_manifest(paths["manifest_csv"])
    if not rows:
        return
    for row in rows:
        row["config_hash"] = config_hash
    write_manifest(paths["manifest_csv"], rows)


def _preflight(project_dir: Path, cli_config: dict | None) -> tuple[dict[str, Path], dict, str, dict, list[str]]:
    paths = _project_paths(project_dir)
    config, warnings = _load_project_config(project_dir, cli_config)
    config_hash = _config_hash(config)

    _ensure_dirs(paths)
    if not paths["manifest_csv"].exists():
        write_manifest(paths["manifest_csv"], [])

    _validate_project_contract(paths, config, warnings)
    versions = _run_preflight_checks(project_dir, config, warnings)

    config_hash = _config_hash(config)
    _write_preflight_log(paths, config, config_hash, versions, warnings)
    return paths, config, config_hash, versions, warnings




def _write_failure_preflight_log(paths: dict[str, Path], config: dict, error: Exception) -> None:
    lines = [
        f"python_executable={sys.executable}",
        f"python_version={sys.version}",
        f"platform={platform.platform()}",
        f"cwd={Path.cwd()}",
        f"project_dir={paths['project']}",
        f"config_hash={_config_hash(config)}",
        f"error={error}",
    ]
    paths["preflight_log"].parent.mkdir(parents=True, exist_ok=True)
    paths["preflight_log"].write_text("\n".join(lines) + "\n", encoding="utf-8", errors="replace")

def _history_append(status_path: Path, entry: dict) -> None:
    status = read_run_status(status_path)
    history = list(status.get("history", []))
    history.append(entry)
    update_run_status(status_path, history=history)


def _run_docking(project_dir: Path, logs_dir: Path, config: dict):
    mode = (config.get("docking_mode") or "cpu").lower()
    if mode == "gpu":
        return docking_gpu.run(project_dir, logs_dir, vina_path=config.get("resolved_vina_gpu_path"))
    return docking_cpu.run(project_dir, logs_dir, vina_path=config.get("resolved_vina_cpu_path"))


def _module_is_complete_for_all_ligands(paths: dict[str, Path], module_name: str) -> bool:
    rows = read_manifest(paths["manifest_csv"])
    if not rows:
        return False

    field_map = {
        "module1_admet": "admet_status",
        "module2_build3d": "sdf_status",
        "module3_meeko": "pdbqt_status",
        "module4_docking": "vina_status",
    }
    field = field_map[module_name]
    return all((row.get(field) or "").upper() in {"PASS", "DONE", "OK", "SUCCESS"} for row in rows)


def _preflight_failure(project_dir: Path, cli_config: dict | None, error: Exception) -> dict:
    paths = _project_paths(project_dir)
    _ensure_dirs(paths)
    config, _ = _load_project_config(project_dir, cli_config)
    _write_failure_preflight_log(paths, config, error)
    status = update_run_status(
        paths["status_json"],
        phase="failed",
        failed_module="preflight",
        completed_modules=[],
        config_snapshot=config,
        config_hash=_config_hash(config),
        error=str(error),
        runtime=_runtime_info(),
        resolved_vina_cpu_path=config.get("resolved_vina_cpu_path"),
        resolved_vina_gpu_path=config.get("resolved_vina_gpu_path"),
        resolved_receptor_path=config.get("resolved_receptor_path"),
    )
    return {
        "ok": False,
        "failed_module": "preflight",
        "error": str(error),
        "results": [],
        "status": status,
        "manifest_rows": len(read_manifest(paths["manifest_csv"])),
    }


def _execute(project_dir: Path, cli_config: dict | None, resume_mode: bool) -> dict:
    try:
        paths, config, config_hash, versions, warnings = _preflight(project_dir, cli_config)
    except PreflightError as exc:
        return _preflight_failure(project_dir, cli_config, exc)

    status_path = paths["status_json"]
    current = read_run_status(status_path)
    completed = set(current.get("completed_modules", [])) if resume_mode else set()

    update_run_status(
        status_path,
        phase="running",
        failed_module=None,
        completed_modules=sorted(completed),
        config_snapshot=config,
        config_hash=config_hash,
        warnings=warnings,
        tool_versions=versions,
        runtime=_runtime_info(),
        resolved_vina_cpu_path=config.get("resolved_vina_cpu_path"),
        resolved_vina_gpu_path=config.get("resolved_vina_gpu_path"),
        resolved_receptor_path=config.get("resolved_receptor_path"),
    )

    results = []
    for module_name in MODULES:
        if module_name in completed:
            continue
        if resume_mode and _module_is_complete_for_all_ligands(paths, module_name):
            completed.add(module_name)
            continue

        if module_name == "module1_admet":
            result = admet.run(project_dir, paths["engine_logs_dir"])
        elif module_name == "module2_build3d":
            result = build3d.run(project_dir, paths["engine_logs_dir"])
        elif module_name == "module3_meeko":
            result = meeko.run(project_dir, paths["engine_logs_dir"])
        else:
            result = _run_docking(project_dir, paths["engine_logs_dir"], config)

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
            _stamp_manifest_config_hash(paths, config_hash)
            return {
                "ok": False,
                "failed_module": module_name,
                "results": results,
                "status": read_run_status(status_path),
                "manifest_rows": len(read_manifest(paths["manifest_csv"])),
            }

        completed.add(module_name)
        update_run_status(status_path, completed_modules=sorted(completed), runtime=_runtime_info())

    _stamp_manifest_config_hash(paths, config_hash)
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
    return _execute(project_dir=project_dir, cli_config=config, resume_mode=False)


def validate(project_dir: Path, config: dict | None = None) -> dict:
    try:
        _, resolved, config_hash, versions, warnings = _preflight(project_dir, config)
        return {
            "ok": True,
            "config_snapshot": resolved,
            "config_hash": config_hash,
            "tool_versions": versions,
            "warnings": warnings,
            "runtime": _runtime_info(),
        }
    except PreflightError as exc:
        paths = _project_paths(project_dir)
        _ensure_dirs(paths)
        resolved, _ = _load_project_config(project_dir, config)
        _write_failure_preflight_log(paths, resolved, exc)
        return {
            "ok": False,
            "error": str(exc),
            "runtime": _runtime_info(),
        }


def resume(project_dir: Path) -> dict:
    current = read_run_status(project_dir / "state" / "run_status.json")
    config = current.get("config_snapshot") or current.get("config") or {}
    return _execute(project_dir=project_dir, cli_config=config, resume_mode=True)


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

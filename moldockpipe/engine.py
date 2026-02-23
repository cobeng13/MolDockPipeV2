from __future__ import annotations

import csv
import hashlib
import importlib
import importlib.metadata
import importlib.util
import json
import platform
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from moldockpipe.adapters import admet, build3d, docking_cpu, docking_gpu, meeko
from moldockpipe.adapters.common import REPO_ROOT
from moldockpipe.state import read_manifest, read_run_status, update_run_status, write_manifest

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

MODULES: list[str] = ["module1_admet", "module2_build3d", "module3_meeko", "module4_docking"]
MODULE_LABELS = {
    "module1_admet": "Running Module 1/4: ADMET screening",
    "module2_build3d": "Running Module 2/4: RDKit 3D building",
    "module3_meeko": "Running Module 3/4: Meeko ligand prep",
    "module4_docking": "Running Module 4/4: Vina docking",
}

DEFAULT_CONFIG = {
    "docking_mode": "cpu",
    "strict_versions": False,
    "receptor_path": "receptors/target_prepared.pdbqt",
    "tools": {"vina_cpu_path": "tools/vina_1.2.7_win.exe", "vina_gpu_path": "tools/vina-gpu.exe"},
    "docking": {
        "box": {"center": [0.0, 0.0, 0.0], "size": [20.0, 20.0, 20.0]},
        "exhaustiveness": 8,
        "num_modes": 9,
        "energy_range": 3,
    },
}
CPU_VINA_CANDIDATES = ["vina", "vina.exe", "vina_1.2.7_win.exe", "vina_1.2.5_win.exe"]
GPU_VINA_CANDIDATES = ["Vina-GPU+.exe", "Vina-GPU+_K.exe", "Vina-GPU.exe", "vina-gpu.exe", "vina-gpu"]
RECOMMENDED = {"python": "3.11", "rdkit": "2025.03.", "meeko": "0.6.1"}


class PreflightError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


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


def _new_run_id(config_hash: str) -> str:
    return f"{_compact_now()}_{config_hash[:8]}"


def _project_paths(project_dir: Path) -> dict[str, Path]:
    project_dir = project_dir.resolve()
    return {
        "project": project_dir,
        "input_csv": project_dir / "input" / "input.csv",
        "state_dir": project_dir / "state",
        "status_json": project_dir / "state" / "run_status.json",
        "runs_dir": project_dir / "state" / "runs",
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
    return {
        "python": platform.python_version(),
        "rdkit": _import_version("rdkit"),
        "meeko": _import_version("meeko") or _package_version("meeko"),
        "pandas": _import_version("pandas"),
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
        warnings.append(f"Recommended RDKit series is 2025.03.* (detected {versions['rdkit']}).")
    if versions.get("meeko") and not _meeko_matches_recommended(versions["meeko"]):
        warnings.append(f"Recommended Meeko is {RECOMMENDED['meeko']} (detected {versions['meeko']}).")
    return warnings


def normalize_path(project_dir_abs: Path, platform_root_abs: Path, user_path: str | None, mode: str) -> Path | None:
    if not user_path:
        return None
    p = Path(user_path).expanduser()
    if p.is_absolute():
        return p.resolve()
    project_candidate = (project_dir_abs / p).resolve()
    if project_candidate.exists() or mode == "receptor":
        return project_candidate
    if mode == "tool":
        platform_candidate = (platform_root_abs / p).resolve()
        if platform_candidate.exists():
            return platform_candidate
    return project_candidate


def _resolve_tool_path(configured: str | None, project_dir: Path, candidates: list[str]) -> tuple[str | None, str | None]:
    project_dir_abs = project_dir.resolve()
    platform_root_abs = REPO_ROOT.resolve()
    if configured:
        resolved = normalize_path(project_dir_abs, platform_root_abs, configured, mode="tool")
        if resolved and resolved.exists():
            return str(resolved), None
        return None, f"Configured tool path not found: {configured}"

    for candidate in candidates:
        for base in (project_dir_abs, platform_root_abs):
            p = (base / candidate).resolve()
            if p.exists():
                return str(p), f"Configured path missing; used fallback candidate '{candidate}'."
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


def _parse_docking_params(config: dict) -> dict:
    docking = config.get("docking") or {}
    box = docking.get("box") or {}
    center = box.get("center")
    size = box.get("size")
    if not isinstance(center, (list, tuple)) or len(center) != 3 or not isinstance(size, (list, tuple)) or len(size) != 3:
        raise PreflightError("Docking parameters missing. Please set docking.box.center and docking.box.size in config/run.yml.")
    try:
        cx, cy, cz = [float(v) for v in center]
        sx, sy, sz = [float(v) for v in size]
        ex = int(docking.get("exhaustiveness", 8))
        nm = int(docking.get("num_modes", 9))
        er = float(docking.get("energy_range", 3))
    except Exception as exc:
        raise PreflightError("Docking parameters missing. Please set docking.box.center and docking.box.size in config/run.yml.") from exc
    if sx <= 0 or sy <= 0 or sz <= 0:
        raise PreflightError("Docking box size values must be > 0.")
    return {
        "center_x": cx,
        "center_y": cy,
        "center_z": cz,
        "size_x": sx,
        "size_y": sy,
        "size_z": sz,
        "exhaustiveness": ex,
        "num_modes": nm,
        "energy_range": er,
    }


def _legacy_vina_config_exists(mode: str, resolved_vina_path: str | None) -> bool:
    if not resolved_vina_path:
        return False
    vp = Path(resolved_vina_path)
    if mode == "gpu":
        return (vp.parent / "VinaGPUConfig.txt").exists() or (vp.parent / "VinaConfig.txt").exists()
    return (vp.parent / "VinaConfig.txt").exists()


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
        f"resolved_vina_cpu={config.get('tools', {}).get('resolved_vina_cpu_path')}",
        f"resolved_vina_gpu={config.get('tools', {}).get('resolved_vina_gpu_path')}",
        f"resolved_docking={config.get('resolved_docking')}",
        f"rdkit_version={versions.get('rdkit')}",
        f"meeko_version={versions.get('meeko')}",
        f"pandas_version={versions.get('pandas')}",
    ]
    for warning in warnings:
        lines.append(f"warning={warning}")
    paths["preflight_log"].parent.mkdir(parents=True, exist_ok=True)
    paths["preflight_log"].write_text("\n".join(lines) + "\n", encoding="utf-8", errors="replace")


def _ensure_dirs(paths: dict[str, Path]) -> None:
    for key in ("state_dir", "runs_dir", "logs_dir", "engine_logs_dir", "results_dir", "structures_dir", "prepared_dir"):
        paths[key].mkdir(parents=True, exist_ok=True)


def _validate_project_contract(paths: dict[str, Path], config: dict, warnings: list[str]) -> None:
    if not paths["project"].exists():
        raise PreflightError(f"project_dir does not exist: {paths['project']}")
    if not paths["input_csv"].exists():
        raise PreflightError(f"Missing required input file: {paths['input_csv']}")
    receptor_cfg = config.get("receptor_path") or "receptors/target_prepared.pdbqt"
    receptor = normalize_path(paths["project"].resolve(), REPO_ROOT.resolve(), receptor_cfg, mode="receptor")
    if receptor is None:
        raise PreflightError("Missing receptor configuration path.")
    config["resolved_receptor_path"] = str(receptor.resolve())
    if not receptor.exists():
        raise PreflightError(f"Missing receptor file: {receptor.resolve()}")
    if not paths["run_yml"].exists():
        warnings.append("config/run.yml missing (allowed).")


def _run_preflight_checks(project_dir: Path, config: dict, warnings: list[str]) -> dict:
    if importlib.util.find_spec("rdkit") is None:
        raise PreflightError("RDKit is required for Module 2. Install RDKit in this environment (recommended: conda-forge, 2025.03.*).")
    if importlib.util.find_spec("meeko") is None:
        raise PreflightError("Meeko is required for Module 3. Install it in this environment: pip install meeko==0.6.1")

    mode = config.get("docking_mode", "cpu")
    tools_cfg = config.setdefault("tools", {})
    if mode == "cpu":
        cpu_path, warn = _resolve_tool_path(tools_cfg.get("vina_cpu_path"), project_dir, CPU_VINA_CANDIDATES)
        tools_cfg["resolved_vina_cpu_path"] = cpu_path
        tools_cfg["resolved_vina_gpu_path"] = None
        if warn:
            warnings.append(warn)
        if not cpu_path:
            attempted = tools_cfg.get("vina_cpu_path") or "tools/vina_1.2.7_win.exe"
            raise PreflightError(
                f"CPU docking selected but no Vina binary was found. Checked configured tools.vina_cpu_path={attempted}. "
                f"Default Option 1 location is {REPO_ROOT / 'tools'}."
            )
    else:
        gpu_path, warn = _resolve_tool_path(tools_cfg.get("vina_gpu_path"), project_dir, GPU_VINA_CANDIDATES)
        tools_cfg["resolved_vina_gpu_path"] = gpu_path
        tools_cfg["resolved_vina_cpu_path"] = None
        if warn:
            warnings.append(warn)
        if not gpu_path:
            attempted = tools_cfg.get("vina_gpu_path") or "tools/vina-gpu.exe"
            raise PreflightError(
                f"GPU docking selected but no Vina-GPU binary was found. Checked configured tools.vina_gpu_path={attempted}. "
                f"Default Option 1 location is {REPO_ROOT / 'tools'}."
            )

    try:
        config["resolved_docking"] = _parse_docking_params(config)
    except PreflightError:
        legacy_ok = _legacy_vina_config_exists(mode, tools_cfg.get("resolved_vina_cpu_path") if mode == "cpu" else tools_cfg.get("resolved_vina_gpu_path"))
        if legacy_ok:
            warnings.append("Using legacy VinaConfig.txt; define docking parameters in run.yml for future compatibility.")
            config["resolved_docking"] = None
        else:
            raise

    versions = _collect_versions()
    warn_versions = _version_warnings(versions)
    warnings.extend(warn_versions)
    if config.get("strict_versions") and warn_versions:
        raise PreflightError("Strict version mode enabled and recommended toolchain versions were not met.")
    return versions


def _stamp_manifest_config_hash(paths: dict[str, Path], config_hash: str) -> None:
    rows = read_manifest(paths["manifest_csv"])
    for row in rows:
        row["config_hash"] = config_hash
    if rows:
        write_manifest(paths["manifest_csv"], rows)


def _archive_previous_status(paths: dict[str, Path]) -> None:
    current = read_run_status(paths["status_json"])
    run_id = current.get("run_id")
    if not run_id:
        return
    archive_dir = paths["runs_dir"] / run_id
    archive_dir.mkdir(parents=True, exist_ok=True)
    (archive_dir / "run_status.json").write_text(json.dumps(current, indent=2), encoding="utf-8")


def _preflight(project_dir: Path, cli_config: dict | None) -> tuple[dict[str, Path], dict, str, dict, list[str]]:
    paths = _project_paths(project_dir)
    config, warnings = _load_project_config(project_dir, cli_config)
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


def _progress(module_name: str, started: bool) -> dict:
    idx = MODULES.index(module_name) + 1
    pct = int(((idx - 1) / len(MODULES)) * 100) if started else int((idx / len(MODULES)) * 100)
    return {"current_module": module_name, "module_index": idx, "module_total": len(MODULES), "percent": pct}


def _read_input_count(input_csv: Path) -> int:
    if not input_csv.exists():
        return 0
    with input_csv.open("r", encoding="utf-8", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def _build_result_summary(paths: dict[str, Path]) -> dict:
    rows = read_manifest(paths["manifest_csv"])
    def cnt(field: str, vals: set[str]) -> int:
        return sum(1 for r in rows if (r.get(field) or "").upper() in vals)
    return {
        "input_rows": _read_input_count(paths["input_csv"]),
        "admet_pass": cnt("admet_status", {"PASS"}),
        "ligands_prepared": cnt("pdbqt_status", {"PASS", "DONE", "OK", "SUCCESS"}),
        "docked_ok": cnt("vina_status", {"DONE", "OK", "SUCCESS"}),
        "docked_failed": cnt("vina_status", {"FAILED"}),
        "leaderboard_csv": str((paths["results_dir"] / "leaderboard.csv").resolve()),
        "summary_csv": str((paths["results_dir"] / "summary.csv").resolve()),
    }


def _run_docking(project_dir: Path, logs_dir: Path, config: dict, config_hash: str):
    mode = (config.get("docking_mode") or "cpu").lower()
    docking_params = config.get("resolved_docking")
    receptor_path = config.get("resolved_receptor_path")
    if mode == "gpu":
        return docking_gpu.run(
            project_dir,
            logs_dir,
            vina_path=config.get("tools", {}).get("resolved_vina_gpu_path"),
            receptor_path=receptor_path,
            docking_params=docking_params,
            config_hash=config_hash,
        )
    return docking_cpu.run(
        project_dir,
        logs_dir,
        vina_path=config.get("tools", {}).get("resolved_vina_cpu_path"),
        receptor_path=receptor_path,
        docking_params=docking_params,
        config_hash=config_hash,
    )


def _preflight_failure(project_dir: Path, cli_config: dict | None, error: Exception) -> dict:
    paths = _project_paths(project_dir)
    _ensure_dirs(paths)
    config, _ = _load_project_config(project_dir, cli_config)
    _archive_previous_status(paths)
    run_id = _new_run_id(_config_hash(config))
    _write_failure_preflight_log(paths, config, error)
    status = update_run_status(
        paths["status_json"],
        run_id=run_id,
        phase="failed",
        phase_detail="Preflight failed",
        progress={"current_module": None, "module_index": 0, "module_total": len(MODULES), "percent": 0},
        failed_module="preflight",
        completed_modules=[],
        config_snapshot=config,
        config_hash=_config_hash(config),
        error=str(error),
        runtime=_runtime_info(),
        history=[],
        result_summary=_build_result_summary(paths),
    )
    return {"ok": False, "failed_module": "preflight", "error": str(error), "results": [], "status": status, "manifest_rows": len(read_manifest(paths["manifest_csv"]))}


def _execute(project_dir: Path, cli_config: dict | None, resume_mode: bool) -> dict:
    try:
        paths, config, config_hash, versions, warnings = _preflight(project_dir, cli_config)
    except PreflightError as exc:
        return _preflight_failure(project_dir, cli_config, exc)

    _archive_previous_status(paths)
    run_id = _new_run_id(config_hash)
    status_path = paths["status_json"]
    current = read_run_status(status_path)
    completed = set(current.get("completed_modules", [])) if resume_mode else set()

    update_run_status(
        status_path,
        run_id=run_id,
        phase="preflight",
        phase_detail="Preflight completed, preparing execution",
        progress={"current_module": None, "module_index": 0, "module_total": len(MODULES), "percent": 0},
        failed_module=None,
        completed_modules=sorted(completed),
        config_snapshot=config,
        config_hash=config_hash,
        warnings=warnings,
        tool_versions=versions,
        runtime=_runtime_info(),
        history=[],
        result_summary=_build_result_summary(paths),
    )

    results = []
    for module_name in MODULES:
        if module_name in completed:
            continue
        if resume_mode and _module_is_complete_for_all_ligands(paths, module_name):
            completed.add(module_name)
            continue

        module_start = datetime.now(timezone.utc)
        update_run_status(
            status_path,
            phase="running",
            phase_detail=MODULE_LABELS[module_name],
            progress=_progress(module_name, started=True),
            completed_modules=sorted(completed),
        )

        if module_name == "module1_admet":
            result = admet.run(project_dir, paths["engine_logs_dir"])
        elif module_name == "module2_build3d":
            result = build3d.run(project_dir, paths["engine_logs_dir"])
        elif module_name == "module3_meeko":
            result = meeko.run(project_dir, paths["engine_logs_dir"])
        else:
            result = _run_docking(project_dir, paths["engine_logs_dir"], config, config_hash)

        module_end = datetime.now(timezone.utc)
        duration = (module_end - module_start).total_seconds()
        record = {
            "run_id": run_id,
            "module": module_name,
            "returncode": result.returncode,
            "stdout_log": result.stdout_log,
            "stderr_log": result.stderr_log,
            "ok": (result.returncode == 0) or (module_name == "module4_docking" and result.returncode == 2),
            "python_executable": sys.executable,
            "started_at": module_start.isoformat().replace("+00:00", "Z"),
            "ended_at": module_end.isoformat().replace("+00:00", "Z"),
            "duration_seconds": round(duration, 3),
        }
        results.append(record)
        status = read_run_status(status_path)
        hist = list(status.get("history", []))
        hist.append(record)
        update_run_status(status_path, history=hist)

        if result.returncode not in (0, 2):
            update_run_status(
                status_path,
                phase="failed",
                phase_detail=f"Failed in {module_name}",
                failed_module=module_name,
                completed_modules=sorted(completed),
                progress=_progress(module_name, started=True),
                runtime=_runtime_info(),
                result_summary=_build_result_summary(paths),
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
        update_run_status(
            status_path,
            completed_modules=sorted(completed),
            progress=_progress(module_name, started=False),
            runtime=_runtime_info(),
        )

        if module_name == "module4_docking" and result.returncode == 2:
            _stamp_manifest_config_hash(paths, config_hash)
            summary = _build_result_summary(paths)
            update_run_status(
                status_path,
                phase="completed_with_errors",
                phase_detail="Pipeline completed with per-ligand docking failures",
                failed_module=None,
                result_summary=summary,
            )
            return {
                "ok": True,
                "warnings": ["Module 4 completed with per-ligand failures."],
                "results": results,
                "status": read_run_status(status_path),
                "manifest_rows": len(read_manifest(paths["manifest_csv"])),
            }

    _stamp_manifest_config_hash(paths, config_hash)
    summary = _build_result_summary(paths)
    update_run_status(
        status_path,
        phase="completed",
        phase_detail="Pipeline completed successfully",
        failed_module=None,
        progress={"current_module": MODULES[-1], "module_index": len(MODULES), "module_total": len(MODULES), "percent": 100},
        runtime=_runtime_info(),
        result_summary=summary,
    )
    return {"ok": True, "results": results, "status": read_run_status(status_path), "manifest_rows": len(read_manifest(paths["manifest_csv"]))}


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
        return {"ok": False, "error": str(exc), "runtime": _runtime_info()}


def resume(project_dir: Path) -> dict:
    current = read_run_status(project_dir / "state" / "run_status.json")
    config = current.get("config_snapshot") or current.get("config") or {}
    return _execute(project_dir=project_dir, cli_config=config, resume_mode=True)


def status(project_dir: Path) -> dict:
    paths = _project_paths(project_dir)
    rs = read_run_status(paths["status_json"])
    last = (rs.get("history") or [])[-1] if rs.get("history") else {}
    return {
        "run_id": rs.get("run_id"),
        "phase": rs.get("phase"),
        "phase_detail": rs.get("phase_detail"),
        "progress": rs.get("progress"),
        "config_hash": rs.get("config_hash"),
        "last_module": last.get("module"),
        "last_returncode": last.get("returncode"),
        "logs_dir": str(paths["engine_logs_dir"]),
        "leaderboard_csv": str((paths["results_dir"] / "leaderboard.csv").resolve()),
        "run_status": rs,
        "manifest_rows": len(read_manifest(paths["manifest_csv"])),
        "project_dir": str(project_dir),
    }


def export_report(project_dir: Path) -> dict:
    rows = read_manifest(project_dir / "state" / "manifest.csv")
    out = project_dir / "results" / "engine_report.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    write_manifest(out, rows)
    return {"rows": len(rows), "report": str(out)}

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
from moldockpipe.state import read_manifest, read_run_status, update_run_status, write_json_atomic, write_manifest

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


def _compact_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _run_id(config_hash: str) -> str:
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
    if configured:
        resolved = normalize_path(project_dir.resolve(), REPO_ROOT.resolve(), configured, mode="tool")
        if resolved and resolved.exists():
            return str(resolved), None
        return None, f"Configured tool path not found: {configured}"
    for candidate in candidates:
        for base in (project_dir.resolve(), REPO_ROOT.resolve()):
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


def _collect_versions() -> dict:
    def modv(name: str):
        try:
            return getattr(importlib.import_module(name), "__version__", None)
        except Exception:
            return None

    def pkgv(name: str):
        try:
            return importlib.metadata.version(name)
        except Exception:
            return None

    return {
        "python": platform.python_version(),
        "rdkit": modv("rdkit"),
        "meeko": modv("meeko") or pkgv("meeko"),
        "pandas": modv("pandas"),
    }


def _version_warnings(versions: dict) -> list[str]:
    warnings = []
    if not str(versions.get("python", "")).startswith(RECOMMENDED["python"]):
        warnings.append(f"Recommended Python is {RECOMMENDED['python']} (detected {versions.get('python')}).")
    if versions.get("rdkit"):
        rd = str(versions["rdkit"]).replace("-", ".")
        if not (rd.startswith("2025.03") or rd.startswith("2025.3")):
            warnings.append(f"Recommended RDKit series is 2025.03.* (detected {versions['rdkit']}).")
    if versions.get("meeko") and str(versions["meeko"]) != RECOMMENDED["meeko"]:
        warnings.append(f"Recommended Meeko is {RECOMMENDED['meeko']} (detected {versions['meeko']}).")
    return warnings


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
    if min(sx, sy, sz) <= 0:
        raise PreflightError("Docking box size values must be > 0.")
    return {
        "box": {"center": [cx, cy, cz], "size": [sx, sy, sz]},
        "docking_params": {"exhaustiveness": ex, "num_modes": nm, "energy_range": er},
    }


def _legacy_vina_config_exists(mode: str, vina_path: str | None) -> bool:
    if not vina_path:
        return False
    vp = Path(vina_path)
    return (vp.parent / "VinaConfig.txt").exists() or (mode == "gpu" and (vp.parent / "VinaGPUConfig.txt").exists())


def _ensure_dirs(paths: dict[str, Path]) -> None:
    for key in ("state_dir", "runs_dir", "logs_dir", "engine_logs_dir", "results_dir", "structures_dir", "prepared_dir"):
        paths[key].mkdir(parents=True, exist_ok=True)


def _validate_contract(paths: dict[str, Path], raw_config: dict, warnings: list[str]) -> tuple[dict, dict]:
    if not paths["project"].exists():
        raise PreflightError(f"project_dir does not exist: {paths['project']}")
    if not paths["input_csv"].exists():
        raise PreflightError(f"Missing required input file: {paths['input_csv']}")

    receptor = normalize_path(paths["project"], REPO_ROOT.resolve(), raw_config.get("receptor_path"), mode="receptor")
    if receptor is None or not receptor.exists():
        raise PreflightError(f"Missing receptor file: {receptor}")

    mode = raw_config.get("docking_mode", "cpu")
    tools_cfg = raw_config.get("tools", {})
    resolved_cpu = resolved_gpu = None
    if mode == "cpu":
        resolved_cpu, warn = _resolve_tool_path(tools_cfg.get("vina_cpu_path"), paths["project"], CPU_VINA_CANDIDATES)
        if warn:
            warnings.append(warn)
        if not resolved_cpu:
            raise PreflightError("CPU docking selected but no Vina binary was found.")
    else:
        resolved_gpu, warn = _resolve_tool_path(tools_cfg.get("vina_gpu_path"), paths["project"], GPU_VINA_CANDIDATES)
        if warn:
            warnings.append(warn)
        if not resolved_gpu:
            raise PreflightError("GPU docking selected but no Vina-GPU binary was found.")

    resolved_docking: dict | None
    try:
        parsed = _parse_docking_params(raw_config)
        resolved_docking = parsed
    except PreflightError:
        legacy_ok = _legacy_vina_config_exists(mode, resolved_cpu if mode == "cpu" else resolved_gpu)
        if legacy_ok:
            warnings.append("Using legacy VinaConfig.txt; define docking parameters in run.yml for future compatibility.")
            resolved_docking = None
        else:
            raise

    resolved = {
        "receptor_path": str(receptor.resolve()),
        "vina_cpu_path": str(Path(resolved_cpu).resolve()) if resolved_cpu else None,
        "vina_gpu_path": str(Path(resolved_gpu).resolve()) if resolved_gpu else None,
        "box": (resolved_docking or {}).get("box"),
        "docking_params": (resolved_docking or {}).get("docking_params"),
    }

    versions = _collect_versions()
    vw = _version_warnings(versions)
    warnings.extend(vw)
    if raw_config.get("strict_versions") and vw:
        raise PreflightError("Strict version mode enabled and recommended toolchain versions were not met.")
    return resolved, versions


def _write_preflight_log(paths: dict[str, Path], run_id: str, config_hash: str, resolved: dict, versions: dict, warnings: list[str]) -> None:
    lines = [
        f"run_id={run_id}",
        f"python_executable={sys.executable}",
        f"python_version={sys.version}",
        f"platform={platform.platform()}",
        f"project_dir={paths['project']}",
        f"config_hash={config_hash}",
        f"resolved={resolved}",
        f"versions={versions}",
    ]
    lines.extend([f"warning={w}" for w in warnings])
    paths["preflight_log"].write_text("\n".join(lines) + "\n", encoding="utf-8", errors="replace")


def _read_input_count(input_csv: Path) -> int:
    if not input_csv.exists():
        return 0
    with input_csv.open("r", encoding="utf-8", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def is_admet_pass(value) -> bool:
    """Normalize legacy/current ADMET pass values from manifest rows."""
    if value is None:
        return False
    s = str(value).strip().upper()
    return s in {"PASS", "PASSED", "OK", "TRUE", "1", "Y", "YES"}


def _build_result_summary(paths: dict[str, Path]) -> dict:
    rows = read_manifest(paths["manifest_csv"])

    def count(field: str, vals: set[str]) -> int:
        return sum(1 for r in rows if (r.get(field) or "").upper() in vals)

    total_rows = len(rows)
    # Accept legacy values (e.g. "PASSED") while canonical writes stay "PASS"/"FAIL".
    admet_pass = sum(1 for r in rows if is_admet_pass(r.get("admet_status")))

    return {
        "input_rows": _read_input_count(paths["input_csv"]),
        "admet_pass": admet_pass,
        "admet_fail": total_rows - admet_pass,
        "ligands_prepared": count("pdbqt_status", {"PASS", "DONE", "OK", "SUCCESS"}),
        "docked_ok": count("vina_status", {"DONE", "OK", "SUCCESS"}),
        "docked_failed": count("vina_status", {"FAILED"}),
        "leaderboard_csv": str((paths["results_dir"] / "leaderboard.csv").resolve()),
        "summary_csv": str((paths["results_dir"] / "summary.csv").resolve()),
    }


def _archive_previous(paths: dict[str, Path]) -> None:
    current = read_run_status(paths["status_json"])
    rid = current.get("run_id")
    if not rid:
        return
    dest = paths["runs_dir"] / rid
    dest.mkdir(parents=True, exist_ok=True)
    write_json_atomic(dest / "run_status.json", current)
    if current.get("config_snapshot") is not None:
        write_json_atomic(dest / "config_snapshot.json", current["config_snapshot"])


def _archive_current(paths: dict[str, Path], status: dict, config_snapshot: dict) -> None:
    rid = status.get("run_id")
    if not rid:
        return
    dest = paths["runs_dir"] / rid
    dest.mkdir(parents=True, exist_ok=True)
    write_json_atomic(dest / "run_status.json", status)
    write_json_atomic(dest / "config_snapshot.json", config_snapshot)


def _init_status(run_id: str, config_hash: str, config_snapshot: dict, resolved: dict, versions: dict, warnings: list[str]) -> dict:
    return {
        "schema_version": "2.0",
        "run_id": run_id,
        "status": "running",
        "result": "success",
        "completed_with_errors": False,
        "started_at": _iso_now(),
        "updated_at": _iso_now(),
        "finished_at": None,
        "phase": "preflight",
        "phase_detail": "Preflight completed",
        "progress": {"current_module": "preflight", "module_index": 0, "module_total": len(MODULES), "percent": 0},
        "completed_modules": [],
        "failed_module": None,
        "config_snapshot": config_snapshot,
        "config_hash": config_hash,
        "resolved": resolved,
        "tool_versions": versions,
        "warnings": warnings,
        "runtime": _runtime_info(),
        "modules": {m: {"status": "pending", "started_at": None, "finished_at": None, "duration_seconds": None} for m in MODULES},
        "history": [],
        "result_summary": {},
    }


def _stamp_manifest_config_hash(paths: dict[str, Path], config_hash: str) -> None:
    rows = read_manifest(paths["manifest_csv"])
    for r in rows:
        r["config_hash"] = config_hash
    if rows:
        write_manifest(paths["manifest_csv"], rows)


def _run_module(module_name: str, project_dir: Path, logs_dir: Path, resolved: dict, config_hash: str):
    if module_name == "module1_admet":
        return admet.run(project_dir, logs_dir)
    if module_name == "module2_build3d":
        return build3d.run(project_dir, logs_dir)
    if module_name == "module3_meeko":
        return meeko.run(project_dir, logs_dir)

    docking = resolved.get("docking_params")
    box = resolved.get("box")
    dock = None
    if docking and box:
        dock = {
            "center_x": box["center"][0],
            "center_y": box["center"][1],
            "center_z": box["center"][2],
            "size_x": box["size"][0],
            "size_y": box["size"][1],
            "size_z": box["size"][2],
            "exhaustiveness": docking["exhaustiveness"],
            "num_modes": docking["num_modes"],
            "energy_range": docking["energy_range"],
        }
    return docking_gpu.run(project_dir, logs_dir, vina_path=resolved.get("vina_gpu_path"), receptor_path=resolved.get("receptor_path"), docking_params=dock, config_hash=config_hash) if resolved.get("vina_gpu_path") else docking_cpu.run(project_dir, logs_dir, vina_path=resolved.get("vina_cpu_path"), receptor_path=resolved.get("receptor_path"), docking_params=dock, config_hash=config_hash)


def _execute(project_dir: Path, cli_config: dict | None, resume_mode: bool) -> dict:
    paths = _project_paths(project_dir)
    _ensure_dirs(paths)
    raw_config, warnings = _load_project_config(paths["project"], cli_config)
    config_snapshot = json.loads(json.dumps(raw_config))
    config_hash = _config_hash(config_snapshot)
    run_id = _run_id(config_hash)

    try:
        resolved, versions = _validate_contract(paths, raw_config, warnings)
        _write_preflight_log(paths, run_id, config_hash, resolved, versions, warnings)
    except PreflightError as exc:
        _archive_previous(paths)
        status = _init_status(run_id, config_hash, config_snapshot, {"receptor_path": None, "vina_cpu_path": None, "vina_gpu_path": None, "box": None, "docking_params": None}, {}, warnings)
        status.update({
            "status": "validation_failed",
            "result": "validation_failed",
            "phase": "failed",
            "phase_detail": "Validation failed",
            "finished_at": _iso_now(),
            "failed_module": "preflight",
            "error": str(exc),
            "result_summary": _build_result_summary(paths),
        })
        write_json_atomic(paths["status_json"], status)
        _archive_current(paths, status, config_snapshot)
        return {"ok": False, "exit_code": 3, "error": str(exc), "status": status}

    _archive_previous(paths)
    status = _init_status(run_id, config_hash, config_snapshot, resolved, versions, warnings)

    # Resume behavior: use previous completed modules if requested
    previous = read_run_status(paths["status_json"])
    completed = set(previous.get("completed_modules", [])) if resume_mode else set()
    status["completed_modules"] = sorted(completed)
    write_json_atomic(paths["status_json"], status)

    for idx, module_name in enumerate(MODULES, start=1):
        if module_name in completed:
            continue
        if resume_mode and _module_is_complete_for_all_ligands(paths, module_name):
            completed.add(module_name)
            status["completed_modules"] = sorted(completed)
            continue

        started = datetime.now(timezone.utc)
        status.update(
            {
                "phase": module_name,
                "phase_detail": MODULE_LABELS[module_name],
                "progress": {
                    "current_module": module_name,
                    "module_index": idx,
                    "module_total": len(MODULES),
                    "percent": int(((idx - 1) / len(MODULES)) * 100),
                },
            }
        )
        status["modules"][module_name]["status"] = "running"
        status["modules"][module_name]["started_at"] = started.isoformat().replace("+00:00", "Z")
        update_run_status(paths["status_json"], **status)

        result = _run_module(module_name, paths["project"], paths["engine_logs_dir"], resolved, config_hash)

        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        rc = result.returncode
        acceptable = (rc == 0) or (module_name == "module4_docking" and rc == 2)

        status["modules"][module_name].update(
            {
                "status": "completed_with_errors" if (module_name == "module4_docking" and rc == 2) else ("completed" if rc == 0 else "failed"),
                "finished_at": ended.isoformat().replace("+00:00", "Z"),
                "duration_seconds": round(duration, 3),
            }
        )
        status["history"] = [
            *status.get("history", []),
            {
                "run_id": run_id,
                "module": module_name,
                "returncode": rc,
                "stdout_log": result.stdout_log,
                "stderr_log": result.stderr_log,
                "started_at": status["modules"][module_name]["started_at"],
                "ended_at": status["modules"][module_name]["finished_at"],
                "duration_seconds": status["modules"][module_name]["duration_seconds"],
            },
        ]

        if not acceptable:
            status.update(
                {
                    "status": "failed",
                    "result": "failed",
                    "phase": "failed",
                    "phase_detail": f"Failed in {module_name}",
                    "failed_module": module_name,
                    "finished_at": _iso_now(),
                    "progress": {
                        "current_module": module_name,
                        "module_index": idx,
                        "module_total": len(MODULES),
                        "percent": int(((idx - 1) / len(MODULES)) * 100),
                    },
                }
            )
            status["result_summary"] = _build_result_summary(paths)
            _stamp_manifest_config_hash(paths, config_hash)
            write_json_atomic(paths["status_json"], status)
            _archive_current(paths, status, config_snapshot)
            return {"ok": False, "exit_code": 1, "failed_module": module_name, "status": status, "results": status["history"]}

        completed.add(module_name)
        status["completed_modules"] = sorted(completed)
        status["progress"] = {
            "current_module": module_name,
            "module_index": idx,
            "module_total": len(MODULES),
            "percent": int((idx / len(MODULES)) * 100),
        }
        update_run_status(paths["status_json"], **status)

        if module_name == "module4_docking" and rc == 2:
            status.update(
                {
                    "status": "completed",
                    "result": "partial_success",
                    "completed_with_errors": True,
                    "phase": "completed",
                    "phase_detail": "Pipeline completed with per-ligand docking failures",
                    "finished_at": _iso_now(),
                }
            )
            status["result_summary"] = _build_result_summary(paths)
            _stamp_manifest_config_hash(paths, config_hash)
            write_json_atomic(paths["status_json"], status)
            _archive_current(paths, status, config_snapshot)
            return {"ok": True, "exit_code": 2, "warnings": ["Module 4 completed with per-ligand failures."], "status": status, "results": status["history"]}

    status.update(
        {
            "status": "completed",
            "result": "success",
            "completed_with_errors": False,
            "phase": "completed",
            "phase_detail": "Pipeline completed successfully",
            "finished_at": _iso_now(),
            "progress": {"current_module": MODULES[-1], "module_index": len(MODULES), "module_total": len(MODULES), "percent": 100},
        }
    )
    status["result_summary"] = _build_result_summary(paths)
    _stamp_manifest_config_hash(paths, config_hash)
    write_json_atomic(paths["status_json"], status)
    _archive_current(paths, status, config_snapshot)
    return {"ok": True, "exit_code": 0, "status": status, "results": status["history"]}


def run(project_dir: Path, config: dict) -> dict:
    return _execute(project_dir=project_dir, cli_config=config, resume_mode=False)


def validate(project_dir: Path, config: dict | None = None) -> dict:
    paths = _project_paths(project_dir)
    _ensure_dirs(paths)
    raw_config, warnings = _load_project_config(paths["project"], config)
    config_snapshot = json.loads(json.dumps(raw_config))
    config_hash = _config_hash(config_snapshot)
    run_id = _run_id(config_hash)
    try:
        resolved, versions = _validate_contract(paths, raw_config, warnings)
        _write_preflight_log(paths, run_id, config_hash, resolved, versions, warnings)
        return {
            "ok": True,
            "exit_code": 0,
            "run_id": run_id,
            "config_snapshot": config_snapshot,
            "resolved": resolved,
            "config_hash": config_hash,
            "tool_versions": versions,
            "warnings": warnings,
            "runtime": _runtime_info(),
        }
    except PreflightError as exc:
        _write_preflight_log(paths, run_id, config_hash, {"receptor_path": None, "vina_cpu_path": None, "vina_gpu_path": None, "box": None, "docking_params": None}, {}, [str(exc)])
        return {"ok": False, "exit_code": 3, "run_id": run_id, "error": str(exc), "runtime": _runtime_info()}


def resume(project_dir: Path) -> dict:
    current = read_run_status((project_dir / "state" / "run_status.json").resolve())
    config = current.get("config_snapshot") or {}
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
        "project_dir": str(paths["project"]),
    }


def export_report(project_dir: Path) -> dict:
    rows = read_manifest((project_dir / "state" / "manifest.csv").resolve())
    out = (project_dir / "results" / "engine_report.csv").resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    write_manifest(out, rows)
    return {"rows": len(rows), "report": str(out)}

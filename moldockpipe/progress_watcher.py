from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path


TERMINAL_PHASES = {"completed", "failed", "validation_failed"}


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(10):
        tmp = path.with_name(f"{path.name}.{os.getpid()}.{attempt}.tmp")
        try:
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_error = exc
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            time.sleep(0.03 * (attempt + 1))
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise
    if last_error is not None:
        raise last_error


def _count_files_with_suffix(folder: Path, suffix: str) -> int:
    if not folder.exists() or not folder.is_dir():
        return 0
    total = 0
    suf = suffix.lower()
    try:
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.lower().endswith(suf):
                    total += 1
    except Exception:
        return 0
    return total


def _count_input_rows(input_csv: Path) -> int | None:
    if not input_csv.exists():
        return None
    try:
        with input_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            _ = next(reader, None)
            count = 0
            for row in reader:
                if row and any(cell.strip() for cell in row):
                    count += 1
            return count
    except Exception:
        return None


def _count_admet(admet_csv: Path) -> tuple[int, int | None]:
    if not admet_csv.exists():
        return 0, None
    passed = 0
    total = 0
    try:
        with admet_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                total += 1
                decision = str(
                    row.get("admet_decision")
                    or row.get("admet_status")
                    or row.get("decision")
                    or ""
                ).strip().upper()
                if decision == "PASS":
                    passed += 1
    except Exception:
        return 0, None
    return passed, total


def _clip_ratio(numerator: int, denominator: int | None) -> float | None:
    if denominator is None or denominator <= 0:
        return None
    value = numerator / denominator
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return round(value, 4)


def _detect_current_module(counts: dict) -> str:
    vina_done = int(counts.get("vina_done") or 0)
    pdbqt = int(counts.get("pdbqt") or 0)
    sdf = int(counts.get("sdf") or 0)
    admet_rows = int(counts.get("admet_rows") or 0)

    if vina_done > 0 or pdbqt > 0:
        return "M4" if vina_done < max(pdbqt, 1) else "M4"
    if pdbqt > 0 or sdf > 0:
        return "M3" if pdbqt < max(sdf, 1) else "M3"
    if sdf > 0:
        return "M2"
    if admet_rows > 0:
        return "M1"
    return "M1"


def _read_phase_from_run_status(status_path: Path) -> tuple[str | None, str]:
    if not status_path.exists():
        return None, ""
    status = _read_json(status_path)
    if not isinstance(status, dict):
        return None, ""
    phase = str(status.get("phase") or "").strip().lower()
    detail = str(status.get("phase_detail") or "").strip()
    if phase in TERMINAL_PHASES:
        if phase == "completed":
            return "completed", detail
        return "failed", detail
    if phase:
        return "running", detail
    return None, detail


def _read_stop_phase(stop_path: Path) -> tuple[str | None, str]:
    if not stop_path.exists():
        return None, ""
    try:
        raw = stop_path.read_text(encoding="utf-8").strip()
    except Exception:
        return "completed", ""
    if not raw:
        return "completed", ""
    parts = [p.strip() for p in raw.split("|")]
    phase = parts[0].lower() if parts else "completed"
    if phase not in {"running", "completed", "failed"}:
        phase = "completed"
    message = parts[1] if len(parts) > 1 else ""
    return phase, message


def _build_payload(project: Path, run_id: str, started_at: float, phase_hint: str, message: str) -> dict:
    input_csv = project / "input" / "input.csv"
    admet_csv = project / "output" / "admet.csv"
    sdf_dir = project / "3D_Structures"
    pdbqt_dir = project / "prepared_ligands"
    results_dir = project / "results"

    total_input = _count_input_rows(input_csv)
    admet_passed, admet_rows = _count_admet(admet_csv)
    sdf_count = _count_files_with_suffix(sdf_dir, ".sdf")
    pdbqt_count = _count_files_with_suffix(pdbqt_dir, ".pdbqt")
    vina_done = _count_files_with_suffix(results_dir, "_out.pdbqt")

    if total_input is None:
        fallback = admet_rows if admet_rows is not None else 0
        if fallback <= 0:
            fallback = sdf_count or pdbqt_count or vina_done or 0
        total_input = fallback if fallback > 0 else None

    m2_denom = admet_passed if admet_passed > 0 else total_input
    m3_denom = sdf_count if sdf_count > 0 else None
    m4_denom = pdbqt_count if pdbqt_count > 0 else None

    counts = {
        "total_input": total_input,
        "admet_passed": admet_passed,
        "sdf": sdf_count,
        "pdbqt": pdbqt_count,
        "vina_done": vina_done,
        "admet_rows": admet_rows,
    }
    progress = {
        "M1": None,
        "M2": _clip_ratio(sdf_count, m2_denom),
        "M3": _clip_ratio(pdbqt_count, m3_denom),
        "M4": _clip_ratio(vina_done, m4_denom),
    }

    current_module = _detect_current_module(counts)
    elapsed = max(0.0, time.monotonic() - started_at)
    return {
        "run_id": run_id,
        "phase": phase_hint,
        "current_module": current_module,
        "timestamp": _iso_now(),
        "elapsed_sec": round(elapsed, 2),
        "counts": {
            "total_input": counts["total_input"],
            "admet_passed": counts["admet_passed"],
            "sdf": counts["sdf"],
            "pdbqt": counts["pdbqt"],
            "vina_done": counts["vina_done"],
        },
        "progress": progress,
        "message": message,
    }


def run_watcher(project: Path, run_id: str, interval_ms: int) -> int:
    progress_path = project / "state" / "progress.json"
    run_status_path = project / "state" / "run_status.json"
    stop_path = project / "state" / "stop_progress_watcher"

    started_at = time.monotonic()
    project = project.resolve()

    if not project.exists():
        payload = {
            "run_id": run_id,
            "phase": "failed",
            "current_module": "M1",
            "timestamp": _iso_now(),
            "elapsed_sec": 0.0,
            "counts": {"total_input": None, "admet_passed": 0, "sdf": 0, "pdbqt": 0, "vina_done": 0},
            "progress": {"M1": None, "M2": None, "M3": None, "M4": None},
            "message": f"Project directory does not exist: {project}",
        }
        _write_json_atomic(progress_path, payload)
        return 1

    initial = {
        "run_id": run_id,
        "phase": "starting",
        "current_module": "M1",
        "timestamp": _iso_now(),
        "elapsed_sec": 0.0,
        "counts": {"total_input": None, "admet_passed": 0, "sdf": 0, "pdbqt": 0, "vina_done": 0},
        "progress": {"M1": None, "M2": None, "M3": None, "M4": None},
        "message": "",
    }
    try:
        _write_json_atomic(progress_path, initial)
    except Exception:
        return 1

    sleep_seconds = max(interval_ms, 200) / 1000.0
    last_payload = initial
    while True:
        stop_phase, stop_message = _read_stop_phase(stop_path)
        run_phase, run_detail = _read_phase_from_run_status(run_status_path)
        phase = "running"
        message = ""
        if run_phase:
            phase = run_phase
            message = run_detail
        if stop_phase:
            phase = stop_phase
            if stop_message:
                message = stop_message

        payload = _build_payload(project, run_id, started_at, phase, message)
        try:
            _write_json_atomic(progress_path, payload)
        except Exception as exc:
            payload["message"] = f"{payload.get('message', '')} write_error={exc}".strip()
            try:
                progress_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            except Exception:
                pass
        last_payload = payload

        if phase in {"completed", "failed"}:
            break
        time.sleep(sleep_seconds)

    # Ensure final state is durable even if loop broke before a write race.
    try:
        _write_json_atomic(progress_path, last_payload)
    except Exception:
        pass
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MolDockPipe progress watcher")
    parser.add_argument("--project", required=True, help="Project root directory")
    parser.add_argument("--run-id", required=True, help="Run identifier for this watcher session")
    parser.add_argument("--interval-ms", type=int, default=500, help="Polling interval in milliseconds")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_watcher(Path(args.project), str(args.run_id), int(args.interval_ms))


if __name__ == "__main__":
    raise SystemExit(main())

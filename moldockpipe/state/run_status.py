from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_run_status(path: Path) -> dict:
    if not path.exists():
        return {
            "phase": "not_started",
            "completed_modules": [],
            "failed_module": None,
            "started_at": None,
            "updated_at": None,
            "history": [],
        }
    return json.loads(path.read_text(encoding="utf-8"))


def write_run_status(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def update_run_status(path: Path, **updates) -> dict:
    status = read_run_status(path)
    if status["started_at"] is None:
        status["started_at"] = _now()
    status.update(updates)
    status["updated_at"] = _now()
    write_run_status(path, status)
    return status

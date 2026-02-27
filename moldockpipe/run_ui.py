from __future__ import annotations

import json
import time
from pathlib import Path

import click

TERMINAL_DONE = {"completed", "failed", "validation_failed"}


def _read_status(status_path: Path) -> dict | None:
    if not status_path.exists():
        return None
    try:
        return json.loads(status_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _fmt_seconds(v) -> str:
    try:
        return f"{float(v):.1f}s"
    except Exception:
        return "-"


def _funnel_line(summary: dict) -> str:
    parts = []
    for key in (
        "input_rows",
        "admet_pass",
        "admet_fail",
        "sdf_done",
        "sdf_failed",
        "pdbqt_done",
        "pdbqt_failed",
        "vina_done",
        "vina_failed",
        "docked_ok",
        "docked_failed",
    ):
        if key in summary:
            parts.append(f"{key}={summary.get(key, 0)}")
    return "  ".join(parts)


def _render_block(status: dict, elapsed_s: float) -> str:
    run_id = status.get("run_id", "-")
    phase = status.get("phase", "-")
    phase_detail = status.get("phase_detail", "")
    state = status.get("status", "-")
    result = status.get("result", "-")

    prog = status.get("progress") or {}
    current_module = prog.get("current_module", "-")
    module_index = prog.get("module_index", "-")
    module_total = prog.get("module_total", "-")
    percent = prog.get("percent", 0)

    lines = [
        "MolDockPipe run (live)",
        f"run_id: {run_id}",
        f"status: {state}   result: {result}",
        f"phase: {phase}",
        f"detail: {phase_detail}",
        f"progress: {percent}%   module: {current_module} ({module_index}/{module_total})",
        f"elapsed: {_fmt_seconds(elapsed_s)}",
    ]

    modules = status.get("modules") or {}
    if modules:
        lines.append("modules:")
        for name in sorted(modules.keys()):
            m = modules.get(name) or {}
            lines.append(f"  - {name}: {m.get('status', '-')}  duration={_fmt_seconds(m.get('duration_seconds'))}")

    summary = status.get("result_summary") or {}
    if summary:
        lines.append("funnel:")
        lines.append(f"  {_funnel_line(summary)}")

    return "\n".join(lines)


def watch_run_status(status_path: Path, poll_interval_s: float = 0.35, startup_wait_s: float = 8.0) -> dict | None:
    start = time.monotonic()
    last = None
    final = None

    while True:
        now = time.monotonic()
        status = _read_status(status_path)
        if status is not None:
            final = status
            block = _render_block(status, elapsed_s=(now - start))
            if block != last:
                click.echo("\x1b[2J\x1b[H" + block, nl=False)
                click.echo()
                last = block
            if str(status.get("status", "")).lower() in TERMINAL_DONE:
                break
        else:
            if (now - start) <= startup_wait_s:
                msg = f"Waiting for run status file: {status_path}"
                if msg != last:
                    click.echo("\x1b[2J\x1b[H" + msg, nl=False)
                    click.echo()
                    last = msg
            else:
                break
        time.sleep(poll_interval_s)

    return final


def render_final_summary(result: dict) -> None:
    status = result.get("status") or {}
    summary = status.get("result_summary") or {}
    click.echo("\nRun complete")
    click.echo(f"status={status.get('status', '-')}, result={status.get('result', '-')}, completed_with_errors={status.get('completed_with_errors', False)}")
    click.echo(f"exit_code={result.get('exit_code', 1)}")
    if summary:
        click.echo(_funnel_line(summary))
        if summary.get("leaderboard_csv"):
            click.echo(f"leaderboard_csv={summary.get('leaderboard_csv')}")
        if summary.get("summary_csv"):
            click.echo(f"summary_csv={summary.get('summary_csv')}")

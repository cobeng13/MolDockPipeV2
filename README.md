# MolDockPipe (Engine + CLI Foundation)

This repository now exposes a **clean orchestration layer** around the original canonical scripts, preparing the project for a future desktop GUI while preserving your core scientific logic.

## What changed

- Added a Python package: `moldockpipe/`
- Added subprocess adapters for canonical modules:
  - `moldockpipe/adapters/admet.py` → `Module 1.py`
  - `moldockpipe/adapters/build3d.py` → `Module 2.py`
  - `moldockpipe/adapters/meeko.py` → `Module 3 (Parallel).py`
  - `moldockpipe/adapters/docking_cpu.py` → `Module 4a (CPU).py`
  - `moldockpipe/adapters/docking_gpu.py` → `Module 4b (GPU)v3.py`
- Added `moldockpipe/engine.py` with:
  - `run(project_dir, config)`
  - `resume(project_dir)`
  - `status(project_dir)`
- Added state helpers under `moldockpipe/state/`
- Added Click CLI with `moldock` command
- Added tests under `tests/`
- Added demo project under `projects/example_project/`

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## CLI usage

```bash
moldock run projects/example_project --docking-mode cpu
moldock resume projects/example_project
moldock status projects/example_project
moldock export-report projects/example_project
```

The CLI prints structured JSON suitable for future GUI integration.

## Orchestration workflow

```text
┌──────────────┐
│ Preflight    │  validate project_dir, input/input.csv, state paths
└──────┬───────┘
       ▼
┌──────────────┐
│ Module 1     │  ADMET adapter
└──────┬───────┘
       ▼
┌──────────────┐
│ Module 2     │  RDKit 3D builder adapter
└──────┬───────┘
       ▼
┌──────────────┐
│ Module 3     │  Meeko adapter
└──────┬───────┘
       ▼
┌──────────────┐
│ Module 4     │  CPU/GPU docking adapter
└──────┬───────┘
       ▼
┌──────────────┐
│ State update │  state/run_status.json + state/manifest.csv + logs/engine/*
└──────────────┘
```

## State + logs

- Run state: `state/run_status.json`
- Manifest: `state/manifest.csv`
- Adapter logs: `logs/engine/<module>.stdout.log` and `logs/engine/<module>.stderr.log`

For GPU runs, stdout/stderr are captured per module execution in these logs.

## GUI integration plan (next step)

The future desktop GUI can call `moldockpipe.engine` functions directly:
- `run(...)` to launch a new project
- `resume(...)` for safe continuation
- `status(...)` for dashboard progress refresh

No GUI is implemented yet by design.

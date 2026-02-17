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

### Recommended Windows setup (single environment, no interpreter mixing)

> Important: install RDKit/Meeko in the **same Python environment** used to run `moldock`.
> Mixing system Python and venv/conda Python is not supported.

```bash
conda create -n moldockpipe python=3.10 -y
conda activate moldockpipe
conda install -c conda-forge rdkit -y
pip install meeko
pip install -e .
```

### venv-only setup (if your platform packages RDKit there)

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
│ Preflight    │  validate project_dir, dependencies, binaries, runtime
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
- Preflight runtime log: `logs/engine/preflight.log`

Runtime behavior:
- All modules are executed with `sys.executable` (the currently active interpreter).
- All modules run with deterministic `cwd=project_dir`.
- Preflight records `python_executable`, `python_version`, and runtime context for reproducibility.

For GPU runs, stdout/stderr are captured per module execution in these logs.

The adapter runner forces UTF-8 for child Python module processes (`PYTHONUTF8=1`, `PYTHONIOENCODING=utf-8`) to avoid Windows ANSI codepage pipe issues (for example emoji output causing cp1252 `UnicodeEncodeError`).

## GUI integration plan (next step)

The future desktop GUI can call `moldockpipe.engine` functions directly:
- `run(...)` to launch a new project
- `resume(...)` for safe continuation
- `status(...)` for dashboard progress refresh

No GUI is implemented yet by design.

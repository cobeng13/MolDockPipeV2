# MolDockPipe (Engine + CLI Foundation)

This repository provides an orchestration layer around canonical Module 1–4 scripts without changing scientific logic.

## Installation (Windows-first)

Use one environment only (no mixing system Python + venv/conda).

```bash
conda env create -f environment.yml
conda activate moldockpipe
python -m pip install -e .
```

Pinned strategy represented in `environment.yml`:
- Python 3.11
- RDKit 2025.03.* (conda-forge, conda-managed)
- Meeko 0.6.1
- Pandas / NumPy / PyYAML / Click

Option 1 (central tools folder):
- Place binaries under platform root: `tools/`
- Example CPU binary: `tools/vina_1.2.7_win.exe`
- Projects stay lightweight (no per-project binary copies required).

## CLI

```bash
moldock --help
moldock validate projects/example_project --docking-mode cpu
moldock run projects/example_project --docking-mode cpu
moldock resume projects/example_project
moldock status projects/example_project
moldock export-report projects/example_project
```

## Config

Project config path: `project_dir/config/run.yml` (optional).

Behavior:
- Defaults are applied when missing.
- YAML values override defaults.
- CLI flags override YAML.
- Resolved snapshot + deterministic `config_hash` (SHA256 canonical JSON) are recorded in `state/run_status.json`.
- `manifest.csv` gets stamped with the same `config_hash`.

## Preflight + reproducibility

Preflight validates:
- `input/input.csv`
- receptor path (`receptor_path`, default `receptors/target_prepared.pdbqt`)
- required output directories (`state/`, `logs/`, `logs/engine/`, `results/`, `3D_Structures/`, `prepared_ligands/`)
- required dependencies for pipeline path (RDKit, Meeko)
- docking binary resolution for configured mode (config-first):
  - `tools.vina_cpu_path` (default `tools/vina_1.2.7_win.exe`)
  - `tools.vina_gpu_path` (default `tools/vina-gpu.exe`)
  - relative path resolution order: project dir override -> platform root -> fallback candidate discovery

Preflight writes UTF-8 audit log to `logs/preflight.log` including:
- `sys.executable`, `sys.version`, platform
- resolved tool paths
- config hash
- detected versions (rdkit/meeko/pandas)
- warnings for non-recommended versions

Recommended versions are warnings by default (strict mode optional via `strict_versions: true`).

## Runtime guarantees

- Module subprocesses always use current interpreter (`sys.executable`).
- Module subprocesses run with deterministic `cwd=project_dir`.
- UTF-8 forced in subprocess env (`PYTHONUTF8=1`, `PYTHONIOENCODING=utf-8`).
- Per-module logs:
  - `logs/engine/<module>.stdout.log`
  - `logs/engine/<module>.stderr.log`
- `run_status.json` includes phase, modules, runtime info, config snapshot/hash, and tool versions.

## Resume behavior

Resume is deterministic and idempotent:
- completed modules are skipped via `completed_modules`
- stage completion in `manifest.csv` is used to skip module re-runs when all ligands already completed for that stage

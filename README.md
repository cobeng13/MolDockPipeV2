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

Docking parameters are GUI-ready in `config/run.yml`:

```yaml
docking:
  box:
    center: [0.0, 0.0, 0.0]
    size: [20.0, 20.0, 20.0]
  exhaustiveness: 8
  num_modes: 9
  energy_range: 3
```

Module 4 no longer requires static `VinaConfig.txt` by default.

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

## UI Progress Tracking (File-Based)

Run progress for the desktop UI is now driven by a dedicated watcher process:

- Watcher module: `python -m moldockpipe.progress_watcher`
- Output file: `project_dir/state/progress.json`
- Write mode: atomic (`progress.json.tmp` -> replace)

During a UI run, Tauri starts:
- runner: `python -m moldockpipe.cli run ...`
- watcher: `python -m moldockpipe.progress_watcher --project <project> --run-id <id> --interval-ms 700`

`progress.json` fields:
- `phase`: `starting | running | completed | failed`
- `current_module`: `M1 | M2 | M3 | M4`
- `elapsed_sec`
- `counts`: `total_input`, `admet_passed`, `sdf`, `pdbqt`, `vina_done`
- `progress`: `M1..M4` (ratio or `null` when denominator is unknown)

Counting is based on disk artifacts (not manifest state):
- `input/input.csv` rows (if present)
- `output/admet.csv` pass decisions
- `3D_Structures/*.sdf`
- `prepared_ligands/*.pdbqt`
- `results/*_out.pdbqt`

To change watcher cadence, update `--interval-ms` in `ui/src-tauri/src/main.rs`.

## Resume behavior

Resume is deterministic and idempotent:
- completed modules are skipped via `completed_modules`
- stage completion in `manifest.csv` is used to skip module re-runs when all ligands already completed for that stage

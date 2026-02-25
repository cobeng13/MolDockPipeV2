# MolDockPipe Desktop UI (Tauri + React)

Windows-first desktop GUI for MolDockPipeV2 with a guided clinician workflow:

1. Select Project
2. Validate
3. Plan
4. Run
5. Results

## Features

- No terminal required for normal operation.
- Uses MolDock CLI as the API boundary (`status`, `validate`, `plan`, `run`).
- Project discovery from:
  - `<repo_root>/projects/*`
  - `%USERPROFILE%/Documents/MolDockPipeV2/Projects/*`
- Recent projects and moldock executable path persisted in local storage.
- Human-readable errors with logs/results actions and diagnostics copy.
- Run guard to prevent double-run.
- Basic manifest (`state/manifest.csv`) table with filtering.

## Prerequisites

- Node.js 18+
- Rust toolchain
- Tauri prerequisites for your platform
- A working `moldock` CLI in PATH **or** set executable path under **Advanced** in app

## Run in development

```bash
cd ui
npm install
npm run tauri dev
```

## Build desktop app

```bash
cd ui
npm install
npm run tauri build
```

## Notes

- The app executes commands such as:
  - `moldock status <project> --json`
  - `moldock validate <project> --json`
  - `moldock plan <project> --json`
  - `moldock run <project> --docking-mode cpu --no-ui --json`
- During runs, the UI polls status every 2.5 seconds.
- Paths are normalized on the frontend for Windows case/slash tolerance.

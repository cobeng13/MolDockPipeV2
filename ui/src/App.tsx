import { useEffect, useMemo, useRef, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { open } from '@tauri-apps/plugin-dialog';
import Papa from 'papaparse';
import { parseJsonFromMixedOutput } from './cliOutput.js';

interface ProjectItem {
  name: string;
  path: string;
  source: string;
}

type JsonObj = Record<string, unknown>;
type ProgressCounts = {
  total_input: number | null;
  admet_passed: number;
  sdf: number;
  pdbqt: number;
  vina_done: number;
};
type ProgressStage = {
  M1: number | null;
  M2: number | null;
  M3: number | null;
  M4: number | null;
};
type ProgressPayload = {
  run_id: string;
  phase: 'starting' | 'running' | 'completed' | 'failed';
  current_module: string;
  timestamp: string;
  elapsed_sec: number;
  counts: ProgressCounts;
  progress: ProgressStage;
  message?: string;
};
type RunJobStatus = {
  found: boolean;
  running: boolean;
  exit_code: number | null;
};


function normalizePath(input: string): string {
  const normalized = input.replace(/\\/g, '/').replace(/\/+$/, '');
  if (/^[A-Z]:/.test(normalized)) {
    return normalized[0].toLowerCase() + normalized.slice(1);
  }
  return normalized;
}

async function runMoldockCommand(
  pythonPath: string,
  projectPath: string,
  args: string[]
): Promise<{ ok: boolean; stdout: string; stderr: string; code: number }> {
  return invoke('run_moldock', {
    pythonPath: pythonPath || null,
    cwd: projectPath,
    args
  });
}

async function runMoldockAsync(
  pythonPath: string,
  projectPath: string,
  args: string[]
): Promise<{ job_id: number; pid: number | null }> {
  return invoke('run_moldock_async', {
    pythonPath: pythonPath || null,
    cwd: projectPath,
    args
  });
}

async function getRunJobStatus(jobId: number): Promise<RunJobStatus | null> {
  try {
    return await invoke<RunJobStatus>('get_run_job_status', { jobId });
  } catch {
    return null;
  }
}

async function readProgressFile(projectPath: string): Promise<ProgressPayload | null> {
  try {
    const text = await invoke<string>('read_progress_file', { projectDir: projectPath });
    const parsed = JSON.parse(text) as ProgressPayload;
    if (!parsed || typeof parsed !== 'object') return null;
    return parsed;
  } catch {
    return null;
  }
}

export function App() {
  const isTauri =
    typeof window !== 'undefined' &&
    Boolean((window as unknown as { __TAURI__?: unknown }).__TAURI__ || (window as unknown as { __TAURI_INTERNALS__?: unknown }).__TAURI_INTERNALS__);
  const [projects, setProjects] = useState<ProjectItem[]>([]);
  const [selectedProject, setSelectedProject] = useState<string>('');
  const [statusData, setStatusData] = useState<JsonObj | null>(null);
  const [planData, setPlanData] = useState<JsonObj | null>(null);
  const [validateData, setValidateData] = useState<JsonObj | null>(null);
  const [runData, setRunData] = useState<JsonObj | null>(null);
  const [error, setError] = useState<string>('');
  const [stderr, setStderr] = useState<string>('');
  const [isRunning, setIsRunning] = useState(false);
  const [dockingMode, setDockingMode] = useState<'cpu' | 'gpu'>('cpu');
  const [pythonPath, setPythonPath] = useState(localStorage.getItem('pythonPath') ?? 'python');
  const [recentProjects, setRecentProjects] = useState<string[]>(JSON.parse(localStorage.getItem('recentProjects') ?? '[]'));
  const [manifestRows, setManifestRows] = useState<Record<string, string>[]>([]);
  const [manifestFilter, setManifestFilter] = useState('ALL');
  const [pollingId, setPollingId] = useState<number | null>(null);
  const [leaderboardRows, setLeaderboardRows] = useState<Record<string, string>[]>([]);
  const [ligandsView, setLigandsView] = useState<'leaderboard' | 'manifest'>('leaderboard');
  const [ligandsPageSize, setLigandsPageSize] = useState(10);
  const [ligandsPage, setLigandsPage] = useState(1);
  const [activePanel, setActivePanel] = useState<'status' | 'validate' | 'plan' | 'run'>('status');
  const [isLoadingStatus, setIsLoadingStatus] = useState(false);
  const [isLoadingPlan, setIsLoadingPlan] = useState(false);
  const [isLoadingValidate, setIsLoadingValidate] = useState(false);
  const [isLoadingManifest, setIsLoadingManifest] = useState(false);
  const [isLoadingLeaderboard, setIsLoadingLeaderboard] = useState(false);
  const isPollingStatusRef = useRef(false);
  const pollingIdRef = useRef<number | null>(null);
  const progressPollingIdRef = useRef<number | null>(null);
  const jobPollingIdRef = useRef<number | null>(null);
  const activeRunIdRef = useRef<string | null>(null);
  const activeJobIdRef = useRef<number | null>(null);
  const [progressData, setProgressData] = useState<ProgressPayload | null>(null);
  const [progressPollingId, setProgressPollingId] = useState<number | null>(null);

  const projectPath = useMemo(() => {
    const known = projects.find((item) => normalizePath(item.path) === normalizePath(selectedProject));
    return known?.path ?? selectedProject;
  }, [projects, selectedProject]);

  useEffect(() => {
    if (!isTauri) {
      setError('Web preview detected. Desktop actions are disabled until you run the Tauri app.');
      return;
    }
    invoke<ProjectItem[]>('discover_projects')
      .then(setProjects)
      .catch((err) => setError(String(err)));
  }, [isTauri]);

  useEffect(() => {
    localStorage.setItem('pythonPath', pythonPath);
  }, [pythonPath]);

  useEffect(() => {
    if (!isTauri) return;
    if (localStorage.getItem('pythonPath')) return;
    invoke<string>('detect_python_path_cmd')
      .then((path) => setPythonPath(path))
      .catch(() => setPythonPath('python'));
  }, [isTauri]);

  useEffect(() => {
    localStorage.setItem('recentProjects', JSON.stringify(recentProjects.slice(0, 8)));
  }, [recentProjects]);

  useEffect(() => {
    if (!projectPath) return;
    if (!isTauri) return;
    loadManifest(projectPath);
  }, [projectPath, isTauri]);

  useEffect(() => {
    if (!projectPath) return;
    if (!isTauri) return;
    loadLeaderboard(projectPath);
  }, [projectPath, isTauri, runData]);

  useEffect(() => {
    if (!projectPath) return;
    if (!isTauri) return;
    refreshStatusPanel();
  }, [projectPath, isTauri]);

  useEffect(() => {
    return () => {
      clearPollingIntervals();
    };
  }, []);

  useEffect(() => {
    if (!isRunning) return;
    const progressPhase = progressData?.phase ?? '';
    const statusPhase = String((statusData?.status as JsonObj | undefined)?.phase ?? '').toLowerCase();
    const terminalProgress = progressPhase === 'completed' || progressPhase === 'failed';
    const terminalStatus =
      statusPhase === 'completed' || statusPhase === 'failed' || statusPhase === 'validation_failed';
    if (!terminalProgress && !terminalStatus) return;

    clearPollingIntervals();
    activeRunIdRef.current = null;
    activeJobIdRef.current = null;
    setIsRunning(false);
  }, [isRunning, progressData, statusData]);

  useEffect(() => {
    setLigandsPage(1);
  }, [ligandsView, ligandsPageSize]);

  async function chooseFolder() {
    if (!isTauri) {
      setError('Choosing a folder is only available in the desktop app.');
      return;
    }
    const selected = await open({ directory: true, multiple: false });
    if (typeof selected === 'string') {
      setSelectedProject(selected);
      setRecentProjects((prev) => [selected, ...prev.filter((item) => normalizePath(item) !== normalizePath(selected))]);
    }
  }

  async function parseJsonCommand(args: string[]): Promise<JsonObj | null> {
    setError('');
    if (!isTauri) {
      setError('This action requires the desktop app. Launch the Tauri build to run commands.');
      return null;
    }
    let result: { ok: boolean; stdout: string; stderr: string; code: number };
    try {
      result = await runMoldockCommand(pythonPath, projectPath, args);
    } catch (err) {
      setError(String(err));
      return null;
    }
    setStderr(result.stderr || result.stdout || '');
    if (!result.ok) {
      const details = result.stderr || result.stdout;
      setError(details || `MolDock command failed (exit code ${result.code}). Verify executable path and project folder.`);
      return null;
    }
    setError('');
    setStderr('');
    const parsed = parseJsonFromMixedOutput(result.stdout);
    if (parsed) {
      return parsed;
    }
    setError('Command succeeded but JSON output could not be parsed.');
    return null;
  }

  async function readRunStatusFile(path: string): Promise<JsonObj | null> {
    try {
      const text = await invoke<string>('read_text_file', { path: `${path}/state/run_status.json` });
      return JSON.parse(text);
    } catch {
      return null;
    }
  }

  function normalizeStatusPayload(data: JsonObj): JsonObj {
    if (!data || typeof data !== 'object') {
      return { status: { status: String(data ?? '') } };
    }
    const candidate = data as Record<string, unknown>;
    const hasSchema =
      'schema_version' in candidate ||
      'phase' in candidate ||
      'modules' in candidate ||
      'progress' in candidate;
    const statusValue = candidate.status;
    if (hasSchema) {
      return { status: candidate };
    }
    if (statusValue && typeof statusValue === 'object') {
      return candidate;
    }
    return { status: { status: String(statusValue ?? '') } };
  }

  async function refreshStatusOnly() {
    if (!projectPath) return;
    if (isPollingStatusRef.current) return;
    isPollingStatusRef.current = true;
    setIsLoadingStatus(true);
    try {
      let data = await readRunStatusFile(projectPath);
      if (!data) {
        data = await parseJsonCommand(['status', projectPath, '--json']);
      }
      if (data) {
        const statusPayload = normalizeStatusPayload(data);
        setStatusData(statusPayload);
        const status = (statusPayload.status as JsonObj | undefined) ?? undefined;
        if (isRunning && !activeRunIdRef.current) {
          const phaseValue = String(status?.phase ?? '').toLowerCase();
          const runId = status?.run_id;
          if (
            typeof runId === 'string' &&
            runId.trim() &&
            phaseValue !== 'completed' &&
            phaseValue !== 'failed' &&
            phaseValue !== 'validation_failed' &&
            phaseValue !== 'not_started'
          ) {
            activeRunIdRef.current = runId;
          }
        }
        if (!isRunning) {
          setActivePanel('status');
        }
        checkRunCompletion(status).catch(() => undefined);
      }
    } finally {
      setIsLoadingStatus(false);
      isPollingStatusRef.current = false;
    }
  }

  async function refreshProgressOnly() {
    if (!projectPath || !isTauri) return;
    const data = await readProgressFile(projectPath);
    if (!data) return;
    setProgressData(data);
    if (isRunning && (data.phase === 'completed' || data.phase === 'failed')) {
      clearPollingIntervals();
      activeRunIdRef.current = null;
      activeJobIdRef.current = null;
      setIsRunning(false);
      await refreshStatusOnly();
      await loadLeaderboard(projectPath);
    }
  }

  async function checkRunCompletion(status: JsonObj | undefined) {
    if (!isRunning) return;
    if (!status) return;
    const phase = String(status?.phase ?? '').toLowerCase();
    if (phase === 'completed' || phase === 'failed' || phase === 'validation_failed') {
      clearPollingIntervals();
      activeRunIdRef.current = null;
      activeJobIdRef.current = null;
      setIsRunning(false);
      setRunData({ status });
      await refreshStatusOnly();
      if (projectPath) {
        await loadLeaderboard(projectPath);
      }
    }
  }

  function clearPollingIntervals() {
    if (pollingIdRef.current !== null) {
      window.clearInterval(pollingIdRef.current);
      pollingIdRef.current = null;
    }
    if (progressPollingIdRef.current !== null) {
      window.clearInterval(progressPollingIdRef.current);
      progressPollingIdRef.current = null;
    }
    if (jobPollingIdRef.current !== null) {
      window.clearInterval(jobPollingIdRef.current);
      jobPollingIdRef.current = null;
    }
    setPollingId(null);
    setProgressPollingId(null);
  }

  async function refreshStatusPanel() {
    if (!projectPath) return;
    await refreshStatusOnly();
    await refreshProgressOnly();
  }

  async function loadPlan() {
    if (!projectPath) return;
    setIsLoadingPlan(true);
    try {
      const data = await parseJsonCommand(['plan', projectPath, '--json']);
      if (data) {
        setPlanData(data);
        setActivePanel('plan');
      }
    } finally {
      setIsLoadingPlan(false);
    }
  }

  async function loadValidate() {
    if (!projectPath) return;
    setIsLoadingValidate(true);
    try {
      const data = await parseJsonCommand(['validate', projectPath, '--json']);
      if (data) {
        setValidateData(data);
        setActivePanel('validate');
      }
    } finally {
      setIsLoadingValidate(false);
    }
  }

  async function runPipeline() {
    if (!projectPath || isRunning) return;
    const errorsFound = Number(((validateData?.validation as JsonObj | undefined)?.summary as JsonObj | undefined)?.errors_found ?? 0);
    if (errorsFound > 0 && !window.confirm('Validation reported issues. Run anyway?')) {
      return;
    }
    setActivePanel('run');
    setIsRunning(true);
    setRunData(null);
    setProgressData(null);
    activeRunIdRef.current = null;
    activeJobIdRef.current = null;
    clearPollingIntervals();
    const runArgs = ['run', projectPath, '--docking-mode', dockingMode, '--no-ui'];
    const statusId = window.setInterval(() => {
      refreshStatusOnly().catch(() => undefined);
    }, 3000);
    const progressId = window.setInterval(() => {
      refreshProgressOnly().catch(() => undefined);
    }, 800);
    const jobId = window.setInterval(async () => {
      if (!activeJobIdRef.current) return;
      const job = await getRunJobStatus(activeJobIdRef.current);
      if (!job || !job.found || job.running) return;
      clearPollingIntervals();
      activeRunIdRef.current = null;
      activeJobIdRef.current = null;
      setIsRunning(false);
      await refreshStatusOnly();
      await refreshProgressOnly();
      if (projectPath) {
        await loadLeaderboard(projectPath);
      }
    }, 1200);
    pollingIdRef.current = statusId;
    progressPollingIdRef.current = progressId;
    jobPollingIdRef.current = jobId;
    setPollingId(statusId);
    setProgressPollingId(progressId);

    try {
      const started = await runMoldockAsync(pythonPath, projectPath, runArgs);
      activeJobIdRef.current = started.job_id;
      setRunData(null);
      window.setTimeout(() => {
        refreshStatusOnly().catch(() => undefined);
      }, 800);
      window.setTimeout(() => {
        refreshProgressOnly().catch(() => undefined);
      }, 900);
    } catch (err) {
      setError(String(err));
      clearPollingIntervals();
      activeRunIdRef.current = null;
      activeJobIdRef.current = null;
      setIsRunning(false);
    }
  }

  async function copyDiagnostics() {
    const report = JSON.stringify({ statusData, planData, validateData, runData }, null, 2);
    await navigator.clipboard.writeText(report);
    alert('Diagnostic report copied.');
  }

  async function openPath(path: string) {
    if (!isTauri) {
      setError('Opening folders is only available in the desktop app.');
      return;
    }
    try {
      await invoke('open_in_explorer', { path });
    } catch (err) {
      setError(String(err));
    }
  }

  async function purgeProject() {
    if (!projectPath) return;
    if (!window.confirm('Purge this project? This will delete generated outputs and reset state.')) {
      return;
    }
    const isDevBuild = Boolean((import.meta as ImportMeta & { env?: { DEV?: boolean } }).env?.DEV);
    if (!isDevBuild) {
      const first = window.prompt("Type 'yes' to confirm purge")?.trim().toLowerCase() ?? '';
      if (first !== 'yes') {
        return;
      }
      const second = window.prompt("Type 'yes' again to confirm")?.trim().toLowerCase() ?? '';
      if (second !== 'yes') {
        return;
      }
    }
    const result = await parseJsonCommand(['purge', projectPath, '--confirm', 'yes', '--confirm2', 'yes']);
    if (result) {
      await refreshStatusPanel();
      await loadManifest(projectPath);
      await loadLeaderboard(projectPath);
      await refreshStatusOnly();
      setProgressData(null);
    }
  }

  function requireProjectPath(action: string): string | null {
    if (!projectPath) {
      setError(`Select a project to ${action}.`);
      return null;
    }
    return projectPath;
  }

  async function loadManifest(path: string) {
    try {
      if (!isTauri) {
        setManifestRows([]);
        return;
      }
      setIsLoadingManifest(true);
      const text = await invoke<string>('read_text_file', { path: `${path}/state/manifest.csv` });
      const parsed = Papa.parse<Record<string, string>>(text, { header: true });
      setManifestRows(parsed.data.filter((row) => Object.keys(row).length > 1));
    } catch (err) {
      setError(String(err));
      setManifestRows([]);
    } finally {
      setIsLoadingManifest(false);
    }
  }

  async function loadLeaderboard(path: string) {
    try {
      if (!isTauri) {
        setLeaderboardRows([]);
        return;
      }
      setIsLoadingLeaderboard(true);
      const text = await invoke<string>('read_text_file', { path: `${path}/results/leaderboard.csv` });
      const parsed = Papa.parse<Record<string, string>>(text, { header: true });
      setLeaderboardRows(parsed.data.filter((row) => Object.keys(row).length > 1));
    } catch (err) {
      setError(String(err));
      setLeaderboardRows([]);
    } finally {
      setIsLoadingLeaderboard(false);
    }
  }

  function ligandIdFromRow(row: Record<string, string>): string | null {
    const candidates = ['id', 'ligand_id', 'ligand', 'name'];
    for (const key of candidates) {
      const value = row[key];
      if (value && value.trim()) return value.trim();
    }
    return null;
  }

  const filteredManifest = manifestRows.filter((row) => {
    if (manifestFilter === 'PASS') return row.admet_status === 'PASS';
    if (manifestFilter === 'FAIL') return Object.values(row).some((val) => val === 'FAILED' || val === 'FAIL');
    if (manifestFilter === 'DONE') return Object.values(row).some((val) => val === 'DONE');
    if (manifestFilter === 'NEEDS_WORK') return Object.values(row).some((val) => !val || val === 'FAILED');
    return true;
  });
  const activeLigandRows = ligandsView === 'leaderboard' ? leaderboardRows : filteredManifest;
  const totalLigandPages = Math.max(1, Math.ceil(activeLigandRows.length / ligandsPageSize));
  const pagedLigandRows = activeLigandRows.slice(
    (ligandsPage - 1) * ligandsPageSize,
    ligandsPage * ligandsPageSize
  );

  const statusObj = statusData?.status as JsonObj | undefined;
  const validation = validateData?.validation as JsonObj | undefined;
  const plan = planData?.plan as JsonObj | undefined;
  const runStatus = (runData?.status as JsonObj | undefined) ?? statusObj;
  const runProgress = (runStatus?.progress as JsonObj | undefined) ?? undefined;
  const runPercent = Number((runProgress?.percent ?? (runStatus as JsonObj | undefined)?.percent ?? 0) as number);
  const runModule = String((runProgress?.current_module ?? (runStatus as JsonObj | undefined)?.current_module ?? (runStatus as JsonObj | undefined)?.phase ?? '-') as string);
  const runPhaseDetail = String(((runStatus as JsonObj | undefined)?.phase_detail ?? '') as string);
  const runModules = (runStatus?.modules as Record<string, JsonObj> | undefined) ?? undefined;
  const runSummary = (runStatus?.result_summary as JsonObj | undefined) ?? undefined;
  const progressCounts = progressData?.counts;
  const progressStage = progressData?.progress;
  const displayModule = progressData?.current_module || runModule;

  function formatElapsedSeconds(status: JsonObj | undefined): string {
    if (!status) return '-';
    const startedAt = status.started_at as string | undefined;
    const updatedAt = status.updated_at as string | undefined;
    if (!startedAt || !updatedAt) return '-';
    const start = Date.parse(startedAt);
    const end = Date.parse(updatedAt);
    if (Number.isNaN(start) || Number.isNaN(end)) return '-';
    const totalSeconds = Math.max(0, Math.floor((end - start) / 1000));
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
  }

  function formatIso(iso: unknown): string {
    if (!iso) return '-';
    return String(iso);
  }

  return (
    <div className="app-shell">
      <header>
        <h1>MolDockPipe Desktop</h1>
        <p>Guided workflow for clinicians: no terminal required.</p>
        {!isTauri && (
          <div className="banner">
            Web preview detected. Launch the Tauri desktop app to enable project discovery and pipeline actions.
          </div>
        )}
      </header>

      <section className="card">
        <h2>Select Project</h2>
        <div className="row">
          <button onClick={chooseFolder} disabled={!isTauri}>Choose Project Folder</button>
          <button
            onClick={() => invoke<ProjectItem[]>('discover_projects').then(setProjects)}
            disabled={!isTauri}
          >
            Auto-discover
          </button>
          <select value={selectedProject} onChange={(e) => setSelectedProject(e.target.value)}>
            <option value="">Select discovered project...</option>
            {projects.map((project) => (
              <option key={project.path} value={project.path}>{project.name} ({project.source})</option>
            ))}
            {recentProjects.map((project) => (
              <option key={project} value={project}>{project} (recent)</option>
            ))}
          </select>
        </div>
        {projectPath && <p><strong>Active project:</strong> {projectPath}</p>}
      </section>

      <section className="card">
        <h2>Dashboard</h2>
        <div className="row wrap">
          <button onClick={refreshStatusPanel} disabled={!isTauri}>Status</button>
          <button onClick={loadValidate} disabled={!isTauri}>Validate</button>
          <button onClick={loadPlan} disabled={!isTauri}>Show Plan</button>
          <button onClick={runPipeline} disabled={!isTauri || isRunning}>{isRunning ? 'Run in progress…' : 'Run'}</button>
          <button onClick={purgeProject} disabled={!isTauri || !projectPath}>Purge Project</button>
          <button
            onClick={() => {
              const base = requireProjectPath('open results');
              if (base) openPath(`${base}/results`);
            }}
            disabled={!isTauri || !projectPath}
          >
            Open Results Folder
          </button>
          <button
            onClick={() => {
              const base = requireProjectPath('open logs');
              if (base) openPath(`${base}/logs`);
            }}
            disabled={!isTauri || !projectPath}
          >
            Open Logs Folder
          </button>
          <button
            onClick={() => {
              const base = requireProjectPath('open the leaderboard file');
              if (base) openPath(`${base}/results/leaderboard.csv`);
            }}
            disabled={!isTauri || !projectPath}
          >
            Open leaderboard.csv
          </button>
          <button
            onClick={() => {
              const base = requireProjectPath('reload leaderboard');
              if (base) loadLeaderboard(base);
            }}
            disabled={!isTauri || !projectPath}
          >
            Reload leaderboard
          </button>
          <button onClick={copyDiagnostics}>Copy Diagnostics</button>
        </div>
        <details>
          <summary>Advanced</summary>
          <label>
            Python executable (dev)
            <input value={pythonPath} onChange={(e) => setPythonPath(e.target.value)} />
          </label>
          <p className="hint">Tip: set this to your conda env, e.g. C:\Miniconda3\envs\moldockpipe\python.exe</p>
          <label>
            Docking mode
            <select value={dockingMode} onChange={(e) => setDockingMode(e.target.value as 'cpu' | 'gpu')}>
              <option value="cpu">CPU (default)</option>
              <option value="gpu">GPU</option>
            </select>
          </label>
        </details>
      </section>

      {validation && activePanel === 'validate' && (
        <section className="card">
          <h2>Validate</h2>
          {isLoadingValidate && (
            <div className="loading">
              <span className="spinner" />
              Refreshing…
            </div>
          )}
          <p>Rows checked: {String((validation.summary as JsonObj | undefined)?.rows_checked ?? '-')}</p>
          <p>Errors found: {String((validation.summary as JsonObj | undefined)?.errors_found ?? '-')}</p>
          {Number((validation.summary as JsonObj | undefined)?.errors_found ?? 0) > 0 && (
            <div className="banner">Validation found issues. You can still run, but review details below.</div>
          )}
          {['manifest_errors', 'artifact_errors', 'fingerprint_mismatches', 'tool_identity_mismatches'].map((key) => (
            <details key={key}>
              <summary>{key}</summary>
              <pre>{JSON.stringify(validation[key], null, 2)}</pre>
            </details>
          ))}
          <details>
            <summary>Raw JSON</summary>
            <pre>{JSON.stringify(validation, null, 2)}</pre>
          </details>
        </section>
      )}

      {plan && activePanel === 'plan' && (
        <section className="card">
          <h2>Plan</h2>
          {isLoadingPlan && (
            <div className="loading">
              <span className="spinner" />
              Refreshing…
            </div>
          )}
          <p>Input IDs: {String(plan.input_ids ?? '-')}</p>
          <p>Module 1 pending: {String(plan.module1_todo ?? '-')}</p>
          <p>Module 2 pending: {String(plan.module2_todo ?? '-')}</p>
          <p>Module 3 pending: {String(plan.module3_todo ?? '-')}</p>
          <p>Module 4 pending: {String(plan.module4_todo ?? '-')}</p>
          <details>
            <summary>Raw JSON</summary>
            <pre>{JSON.stringify(plan, null, 2)}</pre>
          </details>
        </section>
      )}

      {activePanel === 'status' && (
        <section className="card">
          <h2>Status</h2>
          {isLoadingStatus && (
            <div className="loading">
              <span className="spinner" />
              Refreshing…
            </div>
          )}
          {progressData ? (
            <>
              <p>Phase: {progressData.phase}</p>
              <p>Current module: {progressData.current_module}</p>
              <p>Elapsed: {Math.floor(progressData.elapsed_sec)}s</p>
              <p>Total input: {String(progressData.counts.total_input ?? '-')}</p>
              <p>Module 1: ADMET Passed: {progressData.counts.admet_passed}</p>

              <div className="row wrap">
                <span>Module 2: SDF Production:</span>
                <progress
                  value={progressData.progress.M2 !== null ? Math.round(progressData.progress.M2 * 100) : 0}
                  max={100}
                ></progress>
                <span>
                  {progressData.counts.sdf} / {progressData.counts.admet_passed || progressData.counts.total_input || '-'} ({progressData.progress.M2 !== null ? `${Math.round(progressData.progress.M2 * 100)}%` : 'n/a'})
                </span>
              </div>

              <div className="row wrap">
                <span>Module 3: SDF to PDBQT:</span>
                <progress
                  value={progressData.progress.M3 !== null ? Math.round(progressData.progress.M3 * 100) : 0}
                  max={100}
                ></progress>
                <span>
                  {progressData.counts.pdbqt} / {progressData.counts.sdf || '-'} ({progressData.progress.M3 !== null ? `${Math.round(progressData.progress.M3 * 100)}%` : 'n/a'})
                </span>
              </div>

              <div className="row wrap">
                <span>Module 4: Docking:</span>
                <progress
                  value={progressData.progress.M4 !== null ? Math.round(progressData.progress.M4 * 100) : 0}
                  max={100}
                ></progress>
                <span>
                  {progressData.counts.vina_done} / {progressData.counts.pdbqt || '-'} ({progressData.progress.M4 !== null ? `${Math.round(progressData.progress.M4 * 100)}%` : 'n/a'})
                </span>
              </div>
            </>
          ) : (
            <>
              <p>No watcher progress available yet.</p>
              <p className="hint">Start a run to initialize `state/progress.json` tracking.</p>
            </>
          )}
          <details>
            <summary>Raw JSON (Progress)</summary>
            <pre>{JSON.stringify(progressData, null, 2)}</pre>
          </details>
          <details>
            <summary>Raw JSON (Engine Status)</summary>
            <pre>{JSON.stringify(statusObj ?? statusData, null, 2)}</pre>
          </details>
        </section>
      )}

      {activePanel === 'run' && (
        <section className="card">
          <h2>Run</h2>
          <p>Current module: {displayModule}</p>
          <p>Phase: {progressData?.phase ?? String((runStatus as JsonObj | undefined)?.phase ?? '-')}</p>
          {progressData?.message ? <p>{progressData.message}</p> : (runPhaseDetail && <p>{runPhaseDetail}</p>)}
          <p>Last update: {formatIso(progressData?.timestamp ?? (runStatus as JsonObj | undefined)?.updated_at)}</p>
          <p>Elapsed: {progressData ? `${Math.floor(progressData.elapsed_sec)}s` : formatElapsedSeconds(runStatus)}</p>
          {progressStage && (
            <div className="row wrap">
              <p>M2 (SDF): {progressCounts?.sdf ?? 0} / {(progressCounts?.admet_passed ?? 0) || (progressCounts?.total_input ?? '-')} {progressStage.M2 === null ? '' : `(${Math.round(progressStage.M2 * 100)}%)`}</p>
              <p>M3 (PDBQT): {progressCounts?.pdbqt ?? 0} / {progressCounts?.sdf ?? '-'} {progressStage.M3 === null ? '' : `(${Math.round(progressStage.M3 * 100)}%)`}</p>
              <p>M4 (Docking): {progressCounts?.vina_done ?? 0} / {progressCounts?.pdbqt ?? '-'} {progressStage.M4 === null ? '' : `(${Math.round(progressStage.M4 * 100)}%)`}</p>
            </div>
          )}
          <progress value={progressStage?.M4 !== null && progressStage?.M4 !== undefined ? Math.round(progressStage.M4 * 100) : runPercent} max={100}></progress>
          <p>{progressStage?.M4 !== null && progressStage?.M4 !== undefined ? Math.round(progressStage.M4 * 100) : runPercent}%</p>
          {runSummary && (
            <div className="row wrap">
              <p>Input IDs: {String(runSummary['input_rows'] ?? '-')}</p>
              <p>Docked OK: {String(runSummary['docked_ok'] ?? '-')}</p>
              <p>Docked Failed: {String(runSummary['docked_failed'] ?? '-')}</p>
            </div>
          )}
          {runModules && (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Module</th>
                    <th>Status</th>
                    <th>Duration (s)</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(runModules).map(([key, mod]) => (
                    <tr key={key}>
                      <td>{key}</td>
                      <td>{String(mod.status ?? '-')}</td>
                      <td>{String(mod.duration_seconds ?? '-')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {runData && (
            <div>
              <h3>Completion</h3>
              <button
                onClick={() => {
                  const base = requireProjectPath('open the leaderboard file');
                  if (base) openPath(`${base}/results/leaderboard.csv`);
                }}
                disabled={!isTauri}
              >
                Open leaderboard.csv
              </button>
              <button
                onClick={() => {
                  const base = requireProjectPath('open results');
                  if (base) openPath(`${base}/results`);
                }}
                disabled={!isTauri}
              >
                Open results folder
              </button>
              <button onClick={() => navigator.clipboard.writeText(JSON.stringify(runData, null, 2))}>Copy summary JSON</button>
              <details>
                <summary>Raw JSON</summary>
                <pre>{JSON.stringify(runData, null, 2)}</pre>
              </details>
            </div>
          )}
        </section>
      )}

      <section className="card">
        <h2>Docking Results</h2>
        <div className="row">
          <label>
            View
            <select value={ligandsView} onChange={(e) => setLigandsView(e.target.value as 'leaderboard' | 'manifest')}>
              <option value="leaderboard">Leaderboard</option>
              <option value="manifest">Manifest</option>
            </select>
          </label>
          <label>
            Show
            <select value={ligandsPageSize} onChange={(e) => setLigandsPageSize(Number(e.target.value))}>
              {[10, 20, 50, 100].map((size) => (
                <option key={size} value={size}>{size}</option>
              ))}
            </select>
            per page
          </label>
          <select value={manifestFilter} onChange={(e) => setManifestFilter(e.target.value)}>
            <option value="ALL">All</option>
            <option value="PASS">PASS only</option>
            <option value="FAIL">FAIL only</option>
            <option value="DONE">DONE only</option>
            <option value="NEEDS_WORK">Needs work</option>
          </select>
        </div>
        {(isLoadingManifest || isLoadingLeaderboard) && (
          <div className="loading">
            <span className="spinner" />
            Loading data…
          </div>
        )}
        <div className="row">
          <button onClick={() => setLigandsPage((p) => Math.max(1, p - 1))} disabled={ligandsPage <= 1}>Prev</button>
          <span>Page {ligandsPage} of {totalLigandPages}</span>
          <button onClick={() => setLigandsPage((p) => Math.min(totalLigandPages, p + 1))} disabled={ligandsPage >= totalLigandPages}>Next</button>
        </div>
        {ligandsView === 'leaderboard' && leaderboardRows.length > 0 && (() => {
          const columns = Object.keys(leaderboardRows[0] ?? {}).filter((col) => col !== 'pose_path');
          return (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    {columns.map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                    <th>Docking Results</th>
                  </tr>
                </thead>
                <tbody>
                  {pagedLigandRows.map((row, idx) => {
                    const ligandId = ligandIdFromRow(row);
                    return (
                      <tr key={`lb-${idx}`}>
                        {columns.map((column) => (
                          <td key={`${column}-${idx}`}>{row[column]}</td>
                        ))}
                        <td>
                          <button
                            onClick={() => {
                              const base = requireProjectPath('open docking results');
                              if (base && ligandId) openPath(`${base}/results/${ligandId}_out.pdbqt`);
                            }}
                            disabled={!ligandId}
                          >
                            Open PDBQT
                          </button>
                          <button
                            onClick={() => {
                              const base = requireProjectPath('open docking logs');
                              if (base && ligandId) openPath(`${base}/results/${ligandId}_vina.log`);
                            }}
                            disabled={!ligandId}
                          >
                            Open Log
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          );
        })()}
        {ligandsView === 'leaderboard' && leaderboardRows.length === 0 && (
          <p className="hint">No leaderboard data yet. Run docking or reload leaderboard.</p>
        )}
        {ligandsView === 'manifest' && (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {['id', 'admet_status', 'sdf_status', 'pdbqt_status', 'vina_status', 'vina_score'].map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pagedLigandRows.map((row, idx) => (
                  <tr key={`${row.id}-${idx}`}>
                    <td>{row.id}</td>
                    <td>{row.admet_status}</td>
                    <td>{row.sdf_status}</td>
                    <td>{row.pdbqt_status}</td>
                    <td>{row.vina_status}</td>
                    <td>{row.vina_score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {error && (
        <section className="card error">
          <h2>Action needed</h2>
          <p>{error}</p>
          <p>Fix steps: verify project folder, then verify the Python executable path in Advanced.</p>
          <details>
            <summary>stderr details</summary>
            <pre>{stderr || 'No stderr captured.'}</pre>
          </details>
        </section>
      )}

      {(pollingId || progressPollingId) && <p className="hint">Live progress polling enabled.</p>}
    </div>
  );
}

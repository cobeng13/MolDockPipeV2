import { useEffect, useMemo, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { open } from '@tauri-apps/plugin-dialog';
import { readTextFile } from '@tauri-apps/plugin-fs';
import Papa from 'papaparse';

interface ProjectItem {
  name: string;
  path: string;
  source: string;
}

type JsonObj = Record<string, unknown>;

const steps = ['Select Project', 'Validate', 'Plan', 'Run', 'Results'];

function normalizePath(input: string): string {
  const normalized = input.replace(/\\/g, '/').replace(/\/+$/, '');
  if (/^[A-Z]:/.test(normalized)) {
    return normalized[0].toLowerCase() + normalized.slice(1);
  }
  return normalized;
}

async function runMoldockCommand(
  moldockPath: string,
  projectPath: string,
  args: string[]
): Promise<{ ok: boolean; stdout: string; stderr: string; code: number }> {
  return invoke('run_moldock', {
    moldockPath: moldockPath || null,
    cwd: projectPath,
    args
  });
}

export function App() {
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
  const [moldockPath, setMoldockPath] = useState(localStorage.getItem('moldockPath') ?? 'moldock');
  const [recentProjects, setRecentProjects] = useState<string[]>(JSON.parse(localStorage.getItem('recentProjects') ?? '[]'));
  const [manifestRows, setManifestRows] = useState<Record<string, string>[]>([]);
  const [manifestFilter, setManifestFilter] = useState('ALL');
  const [pollingId, setPollingId] = useState<number | null>(null);

  const projectPath = useMemo(() => {
    const known = projects.find((item) => normalizePath(item.path) === normalizePath(selectedProject));
    return known?.path ?? selectedProject;
  }, [projects, selectedProject]);

  useEffect(() => {
    invoke<ProjectItem[]>('discover_projects')
      .then(setProjects)
      .catch((err) => setError(String(err)));
  }, []);

  useEffect(() => {
    localStorage.setItem('moldockPath', moldockPath);
  }, [moldockPath]);

  useEffect(() => {
    localStorage.setItem('recentProjects', JSON.stringify(recentProjects.slice(0, 8)));
  }, [recentProjects]);

  useEffect(() => {
    if (!projectPath) return;
    loadManifest(projectPath);
  }, [projectPath]);

  async function chooseFolder() {
    const selected = await open({ directory: true, multiple: false });
    if (typeof selected === 'string') {
      setSelectedProject(selected);
      setRecentProjects((prev) => [selected, ...prev.filter((item) => normalizePath(item) !== normalizePath(selected))]);
    }
  }

  async function parseJsonCommand(args: string[]): Promise<JsonObj | null> {
    setError('');
    const result = await runMoldockCommand(moldockPath, projectPath, args);
    setStderr(result.stderr || '');
    if (!result.ok) {
      setError(result.stderr || 'MolDock command failed. Verify executable path and project folder.');
      return null;
    }
    try {
      return JSON.parse(result.stdout);
    } catch {
      setError('Command succeeded but JSON output could not be parsed.');
      return null;
    }
  }

  async function refreshStatus() {
    if (!projectPath) return;
    const data = await parseJsonCommand(['status', projectPath, '--json']);
    if (data) setStatusData(data);
  }

  async function loadPlan() {
    if (!projectPath) return;
    const data = await parseJsonCommand(['plan', projectPath, '--json']);
    if (data) setPlanData(data);
  }

  async function loadValidate() {
    if (!projectPath) return;
    const data = await parseJsonCommand(['validate', projectPath, '--json']);
    if (data) setValidateData(data);
  }

  async function runPipeline() {
    if (!projectPath || isRunning) return;
    const errorsFound = Number(((validateData?.validation as JsonObj | undefined)?.summary as JsonObj | undefined)?.errors_found ?? 0);
    if (errorsFound > 0 && !window.confirm('Validation reported issues. Run anyway?')) {
      return;
    }
    setIsRunning(true);
    setRunData(null);
    const runArgs = ['run', projectPath, '--docking-mode', dockingMode, '--no-ui', '--json'];
    const id = window.setInterval(() => {
      refreshStatus().catch(() => undefined);
    }, 2500);
    setPollingId(id);

    try {
      const result = await parseJsonCommand(runArgs);
      if (result) {
        setRunData(result);
        await refreshStatus();
      }
    } finally {
      window.clearInterval(id);
      setPollingId(null);
      setIsRunning(false);
    }
  }

  async function copyDiagnostics() {
    const report = JSON.stringify({ statusData, planData, validateData, runData }, null, 2);
    await navigator.clipboard.writeText(report);
    alert('Diagnostic report copied.');
  }

  async function openPath(path: string) {
    await invoke('open_in_explorer', { path });
  }

  async function loadManifest(path: string) {
    try {
      const text = await readTextFile(`${path}/state/manifest.csv`);
      const parsed = Papa.parse<Record<string, string>>(text, { header: true });
      setManifestRows(parsed.data.filter((row) => Object.keys(row).length > 1));
    } catch {
      setManifestRows([]);
    }
  }

  const filteredManifest = manifestRows.filter((row) => {
    if (manifestFilter === 'PASS') return row.admet_status === 'PASS';
    if (manifestFilter === 'FAIL') return Object.values(row).some((val) => val === 'FAILED' || val === 'FAIL');
    if (manifestFilter === 'DONE') return Object.values(row).some((val) => val === 'DONE');
    if (manifestFilter === 'NEEDS_WORK') return Object.values(row).some((val) => !val || val === 'FAILED');
    return true;
  });

  const statusObj = statusData?.status as JsonObj | undefined;
  const validation = validateData?.validation as JsonObj | undefined;
  const plan = planData?.plan as JsonObj | undefined;
  const runStatus = (runData?.status as JsonObj | undefined) ?? statusObj;

  return (
    <div className="app-shell">
      <header>
        <h1>MolDockPipe Desktop</h1>
        <p>Guided workflow for clinicians: no terminal required.</p>
      </header>

      <section className="steps">
        {steps.map((step) => (
          <div key={step} className="step-card">{step}</div>
        ))}
      </section>

      <section className="card">
        <h2>Select Project</h2>
        <div className="row">
          <button onClick={chooseFolder}>Choose Project Folder</button>
          <button onClick={() => invoke<ProjectItem[]>('discover_projects').then(setProjects)}>Auto-discover</button>
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
          <button onClick={refreshStatus}>Status</button>
          <button onClick={loadValidate}>Validate</button>
          <button onClick={loadPlan}>Show Plan</button>
          <button onClick={runPipeline} disabled={isRunning}>{isRunning ? 'Run in progress…' : 'Run'}</button>
          <button onClick={() => openPath(`${projectPath}/results`)} disabled={!projectPath}>Open Results Folder</button>
          <button onClick={() => openPath(`${projectPath}/logs`)} disabled={!projectPath}>Open Logs Folder</button>
          <button onClick={copyDiagnostics}>Copy Diagnostics</button>
        </div>
        <details>
          <summary>Advanced</summary>
          <label>
            Moldock executable path
            <input value={moldockPath} onChange={(e) => setMoldockPath(e.target.value)} />
          </label>
          <label>
            Docking mode
            <select value={dockingMode} onChange={(e) => setDockingMode(e.target.value as 'cpu' | 'gpu')}>
              <option value="cpu">CPU (default)</option>
              <option value="gpu">GPU</option>
            </select>
          </label>
        </details>
      </section>

      {validation && (
        <section className="card">
          <h2>Validate</h2>
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
        </section>
      )}

      {plan && (
        <section className="card">
          <h2>Plan</h2>
          <pre>{JSON.stringify(plan, null, 2)}</pre>
        </section>
      )}

      <section className="card">
        <h2>Run</h2>
        <p>Current module: {String(runStatus?.current_module ?? runStatus?.phase ?? '-')}</p>
        <p>Elapsed: {String(runStatus?.elapsed_seconds ?? runStatus?.elapsed ?? '-')}</p>
        <progress value={Number(runStatus?.percent ?? 0)} max={100}></progress>
        <p>{Number(runStatus?.percent ?? 0)}%</p>
        {runData && (
          <div>
            <h3>Completion</h3>
            <button onClick={() => openPath(`${projectPath}/results/leaderboard.csv`)}>Open leaderboard.csv</button>
            <button onClick={() => openPath(`${projectPath}/results`)}>Open results folder</button>
            <button onClick={() => navigator.clipboard.writeText(JSON.stringify(runData, null, 2))}>Copy summary JSON</button>
          </div>
        )}
      </section>

      <section className="card">
        <h2>Ligands</h2>
        <div className="row">
          <select value={manifestFilter} onChange={(e) => setManifestFilter(e.target.value)}>
            <option value="ALL">All</option>
            <option value="PASS">PASS only</option>
            <option value="FAIL">FAIL only</option>
            <option value="DONE">DONE only</option>
            <option value="NEEDS_WORK">Needs work</option>
          </select>
        </div>
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
              {filteredManifest.slice(0, 250).map((row, idx) => (
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
      </section>

      {error && (
        <section className="card error">
          <h2>Action needed</h2>
          <p>{error}</p>
          <p>Fix steps: verify project folder, then verify moldock executable path in Advanced.</p>
          <details>
            <summary>stderr details</summary>
            <pre>{stderr || 'No stderr captured.'}</pre>
          </details>
        </section>
      )}

      {pollingId && <p className="hint">Live status polling enabled.</p>}
    </div>
  );
}

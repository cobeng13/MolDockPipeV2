#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

#[derive(Serialize)]
struct ProjectItem {
    name: String,
    path: String,
    source: String,
}

#[derive(Serialize)]
struct RunResult {
    ok: bool,
    stdout: String,
    stderr: String,
    code: i32,
}

#[derive(Serialize, Deserialize)]
struct CsvPreview {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct RunJob {
    job_id: u64,
    pid: Option<u32>,
    watcher_pid: Option<u32>,
}

#[derive(Serialize)]
struct RunJobStatus {
    found: bool,
    running: bool,
    exit_code: Option<i32>,
}

static JOB_COUNTER: AtomicU64 = AtomicU64::new(1);
static JOB_STATUS: OnceLock<Mutex<std::collections::HashMap<u64, Option<i32>>>> = OnceLock::new();

fn job_status_map() -> &'static Mutex<std::collections::HashMap<u64, Option<i32>>> {
    JOB_STATUS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}
fn find_repo_root() -> Option<PathBuf> {
    let mut current = std::env::current_dir().ok()?;
    for _ in 0..6 {
        if current.join("pyproject.toml").exists() {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

fn normalize_path_case(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn with_repo_pythonpath(command: &mut Command) {
    if let Some(repo_root) = find_repo_root() {
        let repo_str = repo_root.to_string_lossy().to_string();
        let merged = match std::env::var("PYTHONPATH") {
            Ok(existing) if !existing.trim().is_empty() => format!("{};{}", repo_str, existing),
            _ => repo_str,
        };
        command.env("PYTHONPATH", merged);
    }
}

fn resolve_project_dir_from_args(args: &[String], cwd: Option<&String>) -> Option<String> {
    if args.len() >= 2 && args[0] == "run" {
        let candidate = args[1].trim();
        if !candidate.is_empty() && !candidate.starts_with('-') {
            return Some(candidate.to_string());
        }
    }
    cwd.cloned()
}

fn write_stop_signal(stop_file: &Path, phase: &str, message: &str) {
    if let Some(parent) = stop_file.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let payload = if message.trim().is_empty() {
        phase.to_string()
    } else {
        format!("{}|{}", phase, message)
    };
    let _ = fs::write(stop_file, payload);
}

fn detect_python_path() -> Option<PathBuf> {
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let candidate = PathBuf::from(prefix).join("python.exe");
        if candidate.exists() {
            return Some(candidate);
        }
    }

    let user = std::env::var("USERPROFILE").ok()?;
    let bases = [
        "Miniconda3",
        "miniconda3",
        "Anaconda3",
        "anaconda3",
        "Mambaforge",
        "mambaforge",
        "Miniforge3",
        "miniforge3",
    ];
    let env_names = ["moldockpipe", "molDockPipe", "base"];
    for base in bases {
        for env_name in env_names {
            let candidate = PathBuf::from(&user)
                .join(base)
                .join("envs")
                .join(env_name)
                .join("python.exe");
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    None
}

#[tauri::command]
fn discover_projects() -> Result<Vec<ProjectItem>, String> {
    let mut project_dirs: Vec<(PathBuf, String)> = Vec::new();

    if let Some(repo_root) = find_repo_root() {
        let root_projects = repo_root.join("projects");
        if root_projects.exists() {
            project_dirs.push((root_projects, "repo projects".to_string()));
        }
    }

    if let Some(home_dir) = std::env::var_os("USERPROFILE") {
        let docs_projects = PathBuf::from(home_dir).join("Documents").join("MolDockPipeV2").join("Projects");
        if docs_projects.exists() {
            project_dirs.push((docs_projects, "Documents".to_string()));
        }
    }

    let mut seen = HashSet::new();
    let mut results = Vec::new();

    for (base, source) in project_dirs {
        let entries = fs::read_dir(base).map_err(|err| err.to_string())?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && path.join("config").exists() {
                let normalized = normalize_path_case(&path).to_ascii_lowercase();
                if !seen.insert(normalized) {
                    continue;
                }
                let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                results.push(ProjectItem {
                    name,
                    path: normalize_path_case(&path),
                    source: source.clone(),
                });
            }
        }
    }

    results.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(results)
}

#[tauri::command]
fn run_moldock(
    args: Vec<String>,
    cwd: Option<String>,
    python_path: Option<String>,
) -> Result<RunResult, String> {
    let python = python_path
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "python".to_string());

    let mut command = Command::new(&python);
    if let Some(run_dir) = cwd {
        // Run relative to the project directory so the CLI can resolve project files.
        command.current_dir(run_dir);
    }
    with_repo_pythonpath(&mut command);
    command.arg("-m").arg("moldockpipe.cli").args(args);

    let output = command.output().map_err(|err| {
        format!(
            "Could not launch Python CLI. Verify Python path and module availability (moldockpipe.cli). Details: {}",
            err
        )
    })?;

    let code = output.status.code().unwrap_or(1);
    Ok(RunResult {
        ok: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        code,
    })
}

#[tauri::command]
fn run_moldock_async(
    args: Vec<String>,
    cwd: Option<String>,
    python_path: Option<String>,
) -> Result<RunJob, String> {
    let python = python_path
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "python".to_string());
    let job_id = JOB_COUNTER.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut map) = job_status_map().lock() {
        map.insert(job_id, None);
    }

    let is_run_command = args.first().map(|v| v == "run").unwrap_or(false);
    if !is_run_command {
        let mut command = Command::new(&python);
        if let Some(run_dir) = cwd {
            command.current_dir(run_dir);
        }
        with_repo_pythonpath(&mut command);
        command.arg("-m").arg("moldockpipe.cli").args(args);
        let mut child = command.spawn().map_err(|err| {
            format!(
                "Could not launch Python CLI. Verify Python path and module availability (moldockpipe.cli). Details: {}",
                err
            )
        })?;
        let pid = child.id();
        let job_id_copy = job_id;
        thread::spawn(move || {
            let code = child.wait().ok().and_then(|s| s.code());
            if let Ok(mut map) = job_status_map().lock() {
                map.insert(job_id_copy, code);
            }
        });
        return Ok(RunJob {
            job_id,
            pid: Some(pid),
            watcher_pid: None,
        });
    }

    let project_dir = resolve_project_dir_from_args(&args, cwd.as_ref())
        .ok_or_else(|| "Unable to resolve project directory for run command.".to_string())?;
    let stop_file = PathBuf::from(&project_dir).join("state").join("stop_progress_watcher");
    if stop_file.exists() {
        let _ = fs::remove_file(&stop_file);
    }

    let watcher_run_id = format!("watch_{}", JOB_COUNTER.load(Ordering::Relaxed));
    let mut watcher = Command::new(&python);
    watcher
        .arg("-m")
        .arg("moldockpipe.progress_watcher")
        .arg("--project")
        .arg(&project_dir)
        .arg("--run-id")
        .arg(&watcher_run_id)
        .arg("--interval-ms")
        .arg("700");
    watcher.current_dir(&project_dir);
    with_repo_pythonpath(&mut watcher);
    let mut watcher_child = watcher.spawn().map_err(|err| {
        format!(
            "Could not launch Python progress watcher. Verify Python path and module availability (moldockpipe.progress_watcher). Details: {}",
            err
        )
    })?;

    let mut command = Command::new(&python);
    if let Some(run_dir) = cwd {
        // Run relative to the project directory so the CLI can resolve project files.
        command.current_dir(run_dir);
    }
    with_repo_pythonpath(&mut command);
    command.arg("-m").arg("moldockpipe.cli").args(args);

    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(err) => {
            let _ = watcher_child.kill();
            let _ = watcher_child.wait();
            return Err(format!(
                "Could not launch Python CLI. Verify Python path and module availability (moldockpipe.cli). Details: {}",
                err
            ));
        }
    };
    let pid = child.id();
    let watcher_pid = watcher_child.id();

    // Detach coordinator thread so UI stays responsive.
    thread::spawn(move || {
        let result = child.wait();
        let (phase, message): (&str, String) = match result {
            Ok(status) => {
                let code = status.code().unwrap_or(1);
                if let Ok(mut map) = job_status_map().lock() {
                    map.insert(job_id, Some(code));
                }
                if code == 0 || code == 2 {
                    ("completed", String::new())
                } else {
                    ("failed", format!("runner_exit_code={}", code))
                }
            }
            Err(_) => {
                if let Ok(mut map) = job_status_map().lock() {
                    map.insert(job_id, Some(1));
                }
                ("failed", "runner_wait_failed".to_string())
            }
        };
        write_stop_signal(&stop_file, phase, &message);

        for _ in 0..20 {
            match watcher_child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        if let Ok(None) = watcher_child.try_wait() {
            let _ = watcher_child.kill();
            let _ = watcher_child.wait();
        }
    });

    Ok(RunJob {
        job_id,
        pid: Some(pid),
        watcher_pid: Some(watcher_pid),
    })
}

#[tauri::command]
fn get_run_job_status(job_id: u64) -> Result<RunJobStatus, String> {
    let map = job_status_map()
        .lock()
        .map_err(|_| "Failed to lock job status map.".to_string())?;
    match map.get(&job_id) {
        None => Ok(RunJobStatus {
            found: false,
            running: false,
            exit_code: None,
        }),
        Some(None) => Ok(RunJobStatus {
            found: true,
            running: true,
            exit_code: None,
        }),
        Some(Some(code)) => Ok(RunJobStatus {
            found: true,
            running: false,
            exit_code: Some(*code),
        }),
    }
}

#[tauri::command]
fn detect_python_path_cmd() -> Result<String, String> {
    Ok(detect_python_path()
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| "python".to_string()))
}

#[tauri::command]
fn open_in_explorer(path: String) -> Result<(), String> {
    if path.trim().is_empty() {
        return Err("No path provided.".to_string());
    }
    let target = PathBuf::from(&path);
    if !target.exists() {
        return Err(format!("Path does not exist: {}", path));
    }
    let resolved = target.canonicalize().map_err(|err| err.to_string())?;

    let mut command = if cfg!(target_os = "windows") {
        let mut cmd = Command::new("explorer");
        if resolved.is_file() {
            cmd.arg("/select,").arg(resolved);
        } else {
            cmd.arg(resolved);
        }
        cmd
    } else if cfg!(target_os = "macos") {
        let mut cmd = Command::new("open");
        cmd.arg(resolved);
        cmd
    } else {
        let mut cmd = Command::new("xdg-open");
        cmd.arg(resolved);
        cmd
    };

    command.spawn().map_err(|err| err.to_string())?;
    Ok(())
}

#[tauri::command]
fn read_text_file(path: String) -> Result<String, String> {
    fs::read_to_string(path).map_err(|err| err.to_string())
}

#[tauri::command]
fn read_progress_file(project_dir: String) -> Result<String, String> {
    let root = PathBuf::from(project_dir)
        .canonicalize()
        .map_err(|err| format!("Invalid project path: {}", err))?;
    let progress_path = root.join("state").join("progress.json");
    let parent = progress_path
        .parent()
        .ok_or_else(|| "Could not resolve progress file parent directory.".to_string())?;
    if !parent.starts_with(&root) {
        return Err("Progress file path rejected (outside project directory).".to_string());
    }
    fs::read_to_string(progress_path).map_err(|err| err.to_string())
}

#[tauri::command]
fn read_csv_preview(path: String, max_rows: usize) -> Result<CsvPreview, String> {
    let mut reader = csv::Reader::from_path(path).map_err(|err| err.to_string())?;
    let headers = reader
        .headers()
        .map_err(|err| err.to_string())?
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<String>>();

    let rows = reader
        .records()
        .take(max_rows)
        .flatten()
        .map(|record| record.iter().map(std::string::ToString::to_string).collect::<Vec<String>>())
        .collect::<Vec<Vec<String>>>();

    Ok(CsvPreview { headers, rows })
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            discover_projects,
            run_moldock,
            detect_python_path_cmd,
            open_in_explorer,
            read_text_file,
            read_progress_file,
            get_run_job_status,
            read_csv_preview,
            run_moldock_async
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

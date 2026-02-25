#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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
    moldock_path: Option<String>,
) -> Result<RunResult, String> {
    let executable = moldock_path
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "moldock".to_string());

    let mut command = Command::new(executable);
    if let Some(run_dir) = cwd {
        command.current_dir(run_dir);
    }
    command.args(args);

    let output = command.output().map_err(|err| {
        format!(
            "Could not launch moldock. Verify moldock is installed or set the executable path in Advanced. Details: {}",
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
fn open_in_explorer(path: String) -> Result<(), String> {
    let mut command = if cfg!(target_os = "windows") {
        let mut cmd = Command::new("explorer");
        cmd.arg(path);
        cmd
    } else if cfg!(target_os = "macos") {
        let mut cmd = Command::new("open");
        cmd.arg(path);
        cmd
    } else {
        let mut cmd = Command::new("xdg-open");
        cmd.arg(path);
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
            open_in_explorer,
            read_text_file,
            read_csv_preview
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

from __future__ import annotations

from pathlib import Path

from .common import AdapterResult, only_ids_env, run_script


def run(
    project_dir: Path,
    logs_dir: Path,
    vina_path: str | None = None,
    receptor_path: str | None = None,
    docking_params: dict | None = None,
    config_hash: str | None = None,
    only_ids: set[str] | None = None,
) -> AdapterResult:
    args: list[str] = []
    env = {}
    if vina_path:
        args.extend(["--vina", vina_path])
        env["MOLDOCK_VINA_GPU_PATH"] = vina_path
    if receptor_path:
        args.extend(["--receptor", receptor_path])
    if docking_params:
        args.extend([
            "--center_x", str(docking_params["center_x"]),
            "--center_y", str(docking_params["center_y"]),
            "--center_z", str(docking_params["center_z"]),
            "--size_x", str(docking_params["size_x"]),
            "--size_y", str(docking_params["size_y"]),
            "--size_z", str(docking_params["size_z"]),
            "--exhaustiveness", str(docking_params["exhaustiveness"]),
            "--num_modes", str(docking_params["num_modes"]),
            "--energy_range", str(docking_params["energy_range"]),
        ])
    if config_hash:
        args.extend(["--config-hash", config_hash])
    ids_env = only_ids_env(project_dir, "module4_docking", only_ids)
    if ids_env:
        env.update(ids_env)

    return run_script("module4b_gpu", "Module 4b (GPU)v3.py", project_dir, logs_dir, args=args, extra_env=env or None)

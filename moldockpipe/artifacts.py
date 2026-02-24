from __future__ import annotations

from pathlib import Path


def sdf_path(project: Path, ligand_id: str) -> Path:
    return (project / "3D_Structures" / f"{ligand_id}.sdf").resolve()


def pdbqt_path(project: Path, ligand_id: str) -> Path:
    return (project / "prepared_ligands" / f"{ligand_id}.pdbqt").resolve()


def vina_out_path(project: Path, ligand_id: str) -> Path:
    return (project / "results" / f"{ligand_id}_out.pdbqt").resolve()


def vina_log_path(project: Path, ligand_id: str) -> Path:
    return (project / "results" / f"{ligand_id}_vina.log").resolve()

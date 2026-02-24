from __future__ import annotations

import hashlib
import json
from pathlib import Path


def stable_hash(obj: dict) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sdf_fp(smiles: str, rdkit_ver: str, params: dict | None = None) -> str:
    return stable_hash({"stage": "sdf", "rdkit": rdkit_ver or "", "smiles": smiles or "", "params": params or {}})


def pdbqt_fp(upstream_sdf_fp: str, meeko_ver: str, params: dict | None = None) -> str:
    return stable_hash({"stage": "pdbqt", "meeko": meeko_ver or "", "upstream_sdf_fp": upstream_sdf_fp or "", "params": params or {}})


def vina_fp(
    upstream_pdbqt_fp: str,
    vina_ver: str,
    receptor_sha1: str,
    docking: dict,
    config_hash: str,
) -> str:
    return stable_hash(
        {
            "stage": "vina",
            "vina": vina_ver or "",
            "receptor_sha1": receptor_sha1 or "",
            "upstream_pdbqt_fp": upstream_pdbqt_fp or "",
            "docking": docking or {},
            "config_hash": config_hash or "",
        }
    )

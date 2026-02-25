from moldockpipe.state.manifest import read_manifest, write_manifest
from moldockpipe.state.run_status import read_run_status, update_run_status


def test_manifest_roundtrip(tmp_path):
    path = tmp_path / "state" / "manifest.csv"
    rows = [{"id": "lig1", "smiles": "CCO", "admet_status": "PASS"}]
    write_manifest(path, rows)
    loaded = read_manifest(path)
    assert loaded[0]["id"] == "lig1"
    assert loaded[0]["admet_status"] == "PASS"


def test_run_status_defaults_and_update(tmp_path):
    path = tmp_path / "state" / "run_status.json"
    default = read_run_status(path)
    assert default["phase"] == "not_started"
    updated = update_run_status(path, phase="running")
    assert updated["phase"] == "running"


def test_manifest_legacy_vina_ver_column_tolerated(tmp_path):
    path = tmp_path / "state" / "manifest.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "id,smiles,vina_status,vina_ver\n"
        "lig1,CCO,DONE,1.2.7\n",
        encoding="utf-8",
    )
    loaded = read_manifest(path)
    assert loaded[0]["id"] == "lig1"
    assert loaded[0].get("vina_ver") == "1.2.7"

    write_manifest(path, loaded)
    header = path.read_text(encoding="utf-8").splitlines()[0]
    assert "vina_ver" not in header

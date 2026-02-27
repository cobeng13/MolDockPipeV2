from pathlib import Path

import click
import pytest

from moldockpipe.purge import validate_project_dir


def test_validate_project_dir_allows_missing_input_csv(tmp_path: Path):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "run.yml").write_text("dock: {}\n", encoding="utf-8")
    # input/input.csv intentionally absent
    validate_project_dir(tmp_path)


def test_validate_project_dir_requires_config_run_yml(tmp_path: Path):
    with pytest.raises(click.ClickException):
        validate_project_dir(tmp_path)

import json
from pathlib import Path

import numpy as np
import pytest
from click.testing import CliRunner

from cwl_registry.cli import main
from cwl_registry.utils import load_json, write_json


def test_from_atlas_density_cli(
    tmp_path, hierarchy_file, annotation_file, density_distribution_file
):
    out_file = str(tmp_path / "summary.json")

    result = CliRunner().invoke(
        main,
        [
            "execute",
            "density-calculation",
            "from-atlas-density",
            "--output-file",
            out_file,
            "--hierarchy",
            hierarchy_file,
            "--annotation",
            annotation_file,
            "--density-distribution",
            density_distribution_file,
        ],
    )
    assert result.exit_code == 0, result.output

    res = load_json(out_file)

    # a rudimentary test. Full test in the statistics unit tests
    assert len(res["hasPart"]) == 2

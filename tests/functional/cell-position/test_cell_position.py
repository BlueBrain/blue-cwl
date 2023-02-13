import subprocess
from pathlib import Path

from click.testing import CliRunner

import pytest

CWD = Path(__file__).parent
EXECUTABLE_PATH = CWD / "run.sh"
OUTPUT_DIR = CWD / "out"


@pytest.fixture(scope="module")
def cell_composition():
    subprocess.run(["/bin/bash", EXECUTABLE_PATH], cwd=CWD, check=True)


def test_cell_composition_completes(cell_composition):
    pass

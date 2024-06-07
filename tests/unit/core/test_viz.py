import pytest
from pathlib import Path
from unittest.mock import patch
from blue_cwl.core import parse_cwl_file


DATA_DIR = Path(__file__).parent / "data/cat-echo"


@pytest.fixture
def workflow():
    return parse_cwl_file(DATA_DIR / "workflow-cat-echo.cwl")


def test_show_workflow_image(workflow):
    """Test that image generation works."""
    with patch("blue_cwl.core.viz.plt.show"):
        workflow.show_image()


def test_write_workflow_image(tmp_path, workflow):
    """Test that image writing works."""
    out_file = tmp_path / "image.png"
    workflow.write_image(filepath=out_file)

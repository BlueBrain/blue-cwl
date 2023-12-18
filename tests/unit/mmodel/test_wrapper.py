from unittest.mock import patch, Mock
from pathlib import Path
import pytest
from cwl_registry.wrappers import mmodel as test_module
from cwl_registry.utils import load_json


DATA_DIR = Path(__file__).parent.parent / "data"


def test_assign_morphologies__raises():
    with pytest.raises(ValueError, match="Both canonical and placeholder nodes are empty."):
        with patch("cwl_registry.wrappers.mmodel._split_circuit", return_value=(None, None)):
            test_module._assign_morphologies(
                None, None, None, None, None, None, None, None, None, None, None
            )


def test_assign_morphologies__only_placeholders(tmp_path):
    """Test branch where no canonicals are selected."""
    placeholder = Mock()
    placeholder.cells.__len__ = Mock(return_value=2)
    canonical = None

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with (
        patch("cwl_registry.wrappers.mmodel._split_circuit", return_value=(None, "nodes")),
        patch("cwl_registry.wrappers.mmodel._run_placeholder_assignment") as patched,
        patch("shutil.move"),
    ):
        test_module._assign_morphologies(
            canonicals=None,
            placeholders="placeholders",
            nodes_file=None,
            population_name=None,
            atlas_info=None,
            output_dir=out_dir,
            output_nodes_file=None,
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
            variant=None,
        )
        patched.assert_called_once_with(
            placeholders="placeholders",
            input_nodes_file="nodes",
            output_morphologies_dir="morph-dir",
            output_nodes_file=out_dir / "placeholders.h5",
        )


def test_assign_morphologies__only_canonicals(tmp_path):
    canonical = Mock()
    canonical.cells.__len__ = Mock(return_value=2)
    placeholder = None

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with (
        patch("cwl_registry.wrappers.mmodel._split_circuit", return_value=("nodes", None)),
        patch("cwl_registry.wrappers.mmodel._run_topological_synthesis") as patched,
        patch("shutil.move"),
    ):
        test_module._assign_morphologies(
            canonicals="canonicals",
            placeholders=None,
            nodes_file="nodes",
            population_name=None,
            atlas_info="atlas-info",
            output_dir=out_dir,
            output_nodes_file="out-nodes",
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
            variant=None,
        )
        patched.assert_called_once_with(
            canonicals="canonicals",
            input_nodes_file="nodes",
            atlas_info="atlas-info",
            output_dir=out_dir,
            output_nodes_file=out_dir / "canonicals.h5",
            output_morphologies_dir="morph-dir",
            seed=10,
            parallel=False,
            variant=None,
        )


def test_assign_morphologies__both_placeholders_canonicals(tmp_path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    placeholder_file = out_dir / "placeholders.h5"
    synthesized_file = out_dir / "canonicals.h5"

    mock = Mock()

    with (
        patch(
            "cwl_registry.wrappers.mmodel._split_circuit",
            return_value=(synthesized_file, placeholder_file),
        ),
        patch("cwl_registry.wrappers.mmodel._run_placeholder_assignment") as place_patched,
        patch("cwl_registry.wrappers.mmodel._run_topological_synthesis") as topo_patched,
        patch("cwl_registry.wrappers.mmodel.merge_cell_collections") as merged_patched,
        patch("voxcell.CellCollection.load_sonata", return_value=mock),
    ):
        test_module._assign_morphologies(
            canonicals="canonicals",
            placeholders="placeholders",
            nodes_file=None,
            population_name="foo",
            atlas_info="atlas-info",
            output_dir=out_dir,
            output_nodes_file="out-nodes",
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
            variant=None,
        )
        topo_patched.assert_called_once_with(
            canonicals="canonicals",
            input_nodes_file=synthesized_file,
            atlas_info="atlas-info",
            output_dir=out_dir,
            output_nodes_file=out_dir / "canonicals.h5",
            output_morphologies_dir="morph-dir",
            seed=10,
            parallel=False,
            variant=None,
        )
        place_patched.assert_called_once_with(
            placeholders="placeholders",
            input_nodes_file=placeholder_file,
            output_morphologies_dir="morph-dir",
            output_nodes_file=out_dir / "placeholders.h5",
        )
        merged_patched.assert_called_once_with(
            splits=[mock, mock],
            population_name="foo",
        )


def test_write_partial_config(tmp_path):
    config = {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": "node_sets.json",
        "networks": {
            "nodes": [
                {
                    "nodes_file": "old-nodes-file",
                    "populations": {
                        "root__neurons": {"type": "biophysical", "partial": ["cell-properties"]}
                    },
                }
            ],
            "edges": [],
        },
        "metadata": {"status": "partial"},
    }

    output_file = tmp_path / "config.json"
    population_name = "root__neurons"
    morphologies_dir = "path-to-morphs"
    nodes_file = "path-to-nodes-file"

    test_module._write_partial_config(
        config, nodes_file, population_name, morphologies_dir, output_file
    )

    res = load_json(output_file)

    assert res == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": "node_sets.json",
        "networks": {
            "nodes": [
                {
                    "nodes_file": "path-to-nodes-file",
                    "populations": {
                        "root__neurons": {
                            "type": "biophysical",
                            "partial": ["cell-properties", "morphologies"],
                            "alternate_morphologies": {
                                "h5v1": "path-to-morphs",
                                "neurolucida-asc": "path-to-morphs",
                            },
                        }
                    },
                }
            ],
            "edges": [],
        },
        "metadata": {"status": "partial"},
    }

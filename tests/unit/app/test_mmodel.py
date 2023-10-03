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
                None, None, None, None, None, None, None, None, None, None
            )


def test_assign_morphologies__only_placeholders():
    """Test branch where no canonicals are selected."""
    placeholder = Mock()
    placeholder.cells.__len__ = Mock(return_value=2)
    canonical = None

    with (
        patch("cwl_registry.wrappers.mmodel._split_circuit", return_value=(canonical, placeholder)),
        patch("cwl_registry.wrappers.mmodel._assign_placeholder_morphologies") as patched,
    ):
        test_module._assign_morphologies(
            None, "placeholders", None, None, None, None, None, "morph-dir", None, None
        )
        patched.assert_called_once_with(
            placeholders="placeholders",
            placeholder_group=placeholder,
            output_morphologies_dir="morph-dir",
        )


def test_assign_morphologies__only_canonicals():
    canonical = Mock()
    canonical.cells.__len__ = Mock(return_value=2)
    placeholder = None

    with (
        patch("cwl_registry.wrappers.mmodel._split_circuit", return_value=(canonical, placeholder)),
        patch("cwl_registry.wrappers.mmodel._run_topological_synthesis") as patched,
    ):
        test_module._assign_morphologies(
            canonicals="canonicals",
            placeholders=None,
            nodes_file="nodes",
            population_name=None,
            atlas_info="atlas-info",
            output_dir="out-dir",
            output_nodes_file="out-nodes",
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
        )
        patched.assert_called_once_with(
            canonicals="canonicals",
            input_nodes_file="nodes",
            atlas_info="atlas-info",
            output_dir="out-dir",
            output_nodes_file="out-nodes",
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
        )


def test_assign_morphologies__both_placeholders_canonicals():
    canonical = Mock()
    canonical.cells.__len__ = Mock(return_value=2)

    placeholder = Mock()
    placeholder.cells.__len__ = Mock(return_value=3)

    with (
        patch("cwl_registry.wrappers.mmodel._split_circuit", return_value=(canonical, placeholder)),
        patch("cwl_registry.wrappers.mmodel._assign_placeholder_morphologies") as place_patched,
        patch("cwl_registry.wrappers.mmodel._run_topological_synthesis") as topo_patched,
        patch("cwl_registry.wrappers.mmodel.merge_cell_collections") as merged_patched,
        patch("voxcell.CellCollection.load_sonata", return_value=canonical.cells),
    ):
        test_module._assign_morphologies(
            canonicals="canonicals",
            placeholders="placeholders",
            nodes_file="nodes",
            population_name="foo",
            atlas_info="atlas-info",
            output_dir=Path("out-dir"),
            output_nodes_file="out-nodes",
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
        )
        topo_patched.assert_called_once_with(
            canonicals="canonicals",
            input_nodes_file=Path("out-dir/canonical_input_nodes.h5"),
            atlas_info="atlas-info",
            output_dir=Path("out-dir"),
            output_nodes_file=Path("out-dir/canonical_output_nodes.h5"),
            output_morphologies_dir="morph-dir",
            parallel=False,
            seed=10,
        )
        place_patched.assert_called_once_with(
            placeholders="placeholders",
            placeholder_group=placeholder,
            output_morphologies_dir="morph-dir",
        )
        merged_patched.assert_called_once_with(
            splits=[canonical, placeholder],
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


import shutil
import libsonata


def test_assign_dymanics(tmp_path):
    nodes_file = DATA_DIR / "nodes_100.h5"

    target_nodes_file = tmp_path / "nodes.h5"

    shutil.copy(nodes_file, target_nodes_file)

    test_module._assign_placeholder_dynamics(target_nodes_file, "root__neurons", 42)

    pop = libsonata.NodeStorage(target_nodes_file).open_population("root__neurons")

    th_current = pop.get_dynamics_attribute("threshold_current", pop.select_all())
    hl_current = pop.get_dynamics_attribute("holding_current", pop.select_all())

    assert len(th_current) == 100
    assert len(hl_current) == 100

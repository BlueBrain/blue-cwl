from unittest.mock import patch, Mock

import inspect
from pathlib import Path
import pytest

import voxcell
import pandas as pd
import pandas.testing as pdt
from blue_cwl.wrappers import mmodel as test_module
from blue_cwl.utils import load_json, create_dir, write_json

from click.testing import CliRunner

DATA_DIR = Path(__file__).parent.parent / "data"


def _check_arg_consistency(cli_command, function):
    """Check that command has the same arguments as the function."""

    cmd_args = set(p.name for p in cli_command.params)
    func_args = set(inspect.signature(function).parameters.keys())

    assert cmd_args == func_args, (
        "Command arguments are not matching function ones:\n"
        f"Command args : {sorted(cmd_args)}\n"
        f"Function args: {sorted(func_args)}"
    )


def test_setup_cli(tmp_path):

    output_dir = create_dir(Path(tmp_path, "out"))

    result = CliRunner().invoke(
        test_module.app,
        [
            "setup",
            "--output-dir",
            str(output_dir)
        ]
    )
    assert result.exit_code == 0
    assert Path(output_dir / "build").is_dir()
    assert Path(output_dir / "stage").is_dir()
    assert Path(output_dir / "transform").is_dir()
    assert Path(output_dir / "build/morphologies").is_dir()


def test_stage_cli():
    """Test that args are passing correctly to the respective function."""
    _check_arg_consistency(test_module.stage_cli, test_module.stage)


def test_split_cli():
    _check_arg_consistency(test_module.split_cli, test_module.split)



@pytest.fixture
def canonical_config():
    return {
  "SSp-bfd2": {
    "L2_TPC:B": {
      "parameters": "mock-params",
      "distributions": "mock-distrs",
      "overrides": {
        "apical_dendrite": {
          "total_extent": None,
          "randomness": 0.28,
          "orientation": None,
          "step_size": {
            "norm": {
              "mean": 1.9,
              "std": 0.2
            }
          },
          "radius": None
        }
      }
    }
  }
}

COLS = [
"region",
"etype",
"hemisphere",
"subregion",
"mtype",
"morph_class",
"synapse_class",
"x",
"y",
"z",
]

@pytest.fixture
def one_region_cells():
    df = pd.DataFrame.from_records([
        ("SSp-bfd2", "cADpyr", "left", "SSp-bfd2", "L2_TPC:B", "PYR", "EXC", 6503., 1012., 2900.),
        ("SSp-bfd2", "cADpyr", "right", "SSp-bfd2", "L2_TPC:B", "PYR", "EXC", 7697., 1306., 9391.),
    ],
        index=pd.RangeIndex(start=1, stop=3),
        columns=COLS,
    )
    return voxcell.CellCollection.from_dataframe(df)


@pytest.fixture
def nodes_file_one_region(tmp_path, one_region_cells):
    filepath = tmp_path / "nodes.h5"
    one_region_cells.save_sonata(filepath)
    return filepath


def test_split__only_canonicals(tmp_path, canonical_config, nodes_file_one_region):
    """Test splitting when canonical config selects all cells leaving no cells for palceholders."""
    output_dir = create_dir(tmp_path / "out")

    canonical_config_file = Path(tmp_path / "canonical.json")
    write_json(data=canonical_config, filepath=canonical_config_file)

    test_module.split(
        canonical_config_file=canonical_config_file,
        nodes_file=nodes_file_one_region,
        output_dir=output_dir,
    )

    expected_canonical_nodes = output_dir / "canonicals.h5"
    assert expected_canonical_nodes.exists()

    expected_placeholder_nodes = output_dir / "placeholders.h5"
    assert expected_placeholder_nodes.exists()

    cells = voxcell.CellCollection.load_sonata(nodes_file_one_region)

    canonicals = voxcell.CellCollection.load_sonata(expected_canonical_nodes)
    assert len(canonicals) == len(cells)
    pdt.assert_frame_equal(canonicals.as_dataframe(), cells.as_dataframe())

    placeholders = voxcell.CellCollection.load_sonata(expected_placeholder_nodes)
    assert len(placeholders) == 0
    assert set(placeholders.as_dataframe().columns) == set(COLS)


def test_split__only_placehoders(tmp_path, canonical_config, nodes_file_one_region):
    """Test splitting when canonical config selects no cells leaving all cells for palceholders."""
    output_dir = create_dir(tmp_path / "out")

    canonical_config["SSp-bfd3"] = canonical_config.pop("SSp-bfd2")

    canonical_config_file = Path(tmp_path / "canonical.json")
    write_json(data=canonical_config, filepath=canonical_config_file)

    test_module.split(
        canonical_config_file=canonical_config_file,
        nodes_file=nodes_file_one_region,
        output_dir=output_dir,
    )

    expected_canonical_nodes = output_dir / "canonicals.h5"
    assert expected_canonical_nodes.exists()

    expected_placeholder_nodes = output_dir / "placeholders.h5"
    assert expected_placeholder_nodes.exists()

    cells = voxcell.CellCollection.load_sonata(nodes_file_one_region)

    canonicals = voxcell.CellCollection.load_sonata(expected_canonical_nodes)
    assert len(canonicals) == 0
    assert set(canonicals.as_dataframe().columns) == set(COLS)

    placeholders = voxcell.CellCollection.load_sonata(expected_placeholder_nodes)
    assert len(placeholders) == len(cells)
    pdt.assert_frame_equal(placeholders.as_dataframe(), cells.as_dataframe())


@pytest.fixture
def two_region_cells():
    df = pd.DataFrame.from_records([
        ("SSp-bfd2", "cADpyr", "left", "SSp-bfd2", "L2_TPC:B", "PYR", "EXC", 6503., 1012., 2900.),
        ("SSp-bfd2", "cADpyr", "left", "SSp-bfd2", "L2_TPC:B", "PYR", "EXC", 6503., 1012., 2900.),
        ("SSp-bfd3", "cADpyr", "right", "SSp-bfd3", "L2_TPC:B", "PYR", "EXC", 7697., 1306., 9391.),
        ("SSp-bfd3", "cADpyr", "right", "SSp-bfd3", "L2_TPC:B", "PYR", "EXC", 7697., 1306., 9391.),
    ],
        index=pd.RangeIndex(start=1, stop=5),
        columns=COLS,
    )
    return voxcell.CellCollection.from_dataframe(df)


@pytest.fixture
def nodes_file_two_regions(tmp_path, two_region_cells):
    filepath = tmp_path / "nodes_two.h5"
    two_region_cells.save_sonata(filepath)
    return filepath


def test_split__both_groups(tmp_path, canonical_config, nodes_file_two_regions):

    output_dir = create_dir(tmp_path / "out")

    canonical_config_file = Path(tmp_path / "canonical.json")
    write_json(data=canonical_config, filepath=canonical_config_file)

    test_module.split(
        canonical_config_file=canonical_config_file,
        nodes_file=nodes_file_two_regions,
        output_dir=output_dir,
    )

    expected_canonical_nodes = output_dir / "canonicals.h5"
    assert expected_canonical_nodes.exists()

    expected_placeholder_nodes = output_dir / "placeholders.h5"
    assert expected_placeholder_nodes.exists()

    cells = voxcell.CellCollection.load_sonata(nodes_file_two_regions)
    df_cells = cells.as_dataframe()

    canonicals = voxcell.CellCollection.load_sonata(expected_canonical_nodes)
    assert canonicals.population_name == cells.population_name
    assert len(canonicals) == 2

    df_canonicals = canonicals.as_dataframe()

    # convert strings into categoricals for the comparison
    for col in df_cells.columns:
        if df_cells[col].dtype == "category":
            df_canonicals[col] = pd.Categorical(df_canonicals[col])

    assert "split_index" in df_canonicals.columns
    pdt.assert_frame_equal(
        df_canonicals.drop(columns="split_index"),
        df_cells.iloc[[0, 1]],
        check_like=True,
    )
    # index to reconstruct the initial order afterwards
    assert df_canonicals.split_index.tolist() == [0, 1]

    placeholders = voxcell.CellCollection.load_sonata(expected_placeholder_nodes)
    assert placeholders.population_name == cells.population_name
    assert len(placeholders) == 2

    df_placeholders = placeholders.as_dataframe()

    # convert strings into categoricals for the comparison
    for col in df_cells.columns:
        if df_cells[col].dtype == "category":
            df_placeholders[col] = pd.Categorical(df_placeholders[col])

    # when saved the index is reset. Bring it back to the df_cells range for comparison
    df_placeholders.index += 2

    assert "split_index" in df_placeholders.columns
    pdt.assert_frame_equal(
        df_placeholders.drop(columns="split_index"),
        df_cells.iloc[[2, 3]],
        check_like=True,
    )
    # index to reconstruct the initial order afterwards
    assert df_placeholders.split_index.tolist() == [2, 3]


def test_transform_cli():
    _check_arg_consistency(test_module.transform_cli, test_module.transform)


def test_assign_placeholders_cli():
    _check_arg_consistency(test_module.assign_placeholders_cli, test_module.assign_placeholders)


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

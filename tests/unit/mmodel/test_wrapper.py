from unittest.mock import patch, Mock


import filecmp
import inspect
import shutil
from pathlib import Path
import pytest

import voxcell
import numpy as np
import pandas as pd
import numpy.testing as npt
import pandas.testing as pdt
from blue_cwl.wrappers import mmodel as test_module
from blue_cwl.utils import load_json, create_dir, write_json
from blue_cwl.exceptions import CWLWorkflowError

from click.testing import CliRunner

DATA_DIR = Path(__file__).parent / "data"


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

    result = CliRunner().invoke(test_module.app, ["setup", "--output-dir", str(output_dir)])
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
                        "step_size": {"norm": {"mean": 1.9, "std": 0.2}},
                        "radius": None,
                    }
                },
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
def nodes_file_one_region(tmp_path):
    # fmt: off
    df = pd.DataFrame.from_records(
        [
            ("SSp-bfd2", "cADpyr", "left", "SSp-bfd2", "L2_TPC:B", "PYR", "EXC", 6503.0, 1012.0, 2900.0),
            ("SSp-bfd2", "cADpyr", "right", "SSp-bfd2", "L2_TPC:B", "PYR", "EXC", 7697.0, 1306.0, 9391.0),
        ],
        index=pd.RangeIndex(start=1, stop=3),
        columns=COLS,
    )
    # fmt: on
    filepath = tmp_path / "nodes.h5"
    voxcell.CellCollection.from_dataframe(df).save_sonata(filepath)
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
    df = pd.DataFrame.from_records(
        [
            (
                "SSp-bfd2",
                "cADpyr",
                "left",
                "SSp-bfd2",
                "L2_TPC:B",
                "PYR",
                "EXC",
                6503.0,
                1012.0,
                2900.0,
            ),
            (
                "SSp-bfd2",
                "cADpyr",
                "left",
                "SSp-bfd2",
                "L2_TPC:B",
                "PYR",
                "EXC",
                6503.0,
                1012.0,
                2900.0,
            ),
            (
                "SSp-bfd3",
                "cADpyr",
                "right",
                "SSp-bfd3",
                "L2_TPC:B",
                "PYR",
                "EXC",
                7697.0,
                1306.0,
                9391.0,
            ),
            (
                "SSp-bfd3",
                "cADpyr",
                "right",
                "SSp-bfd3",
                "L2_TPC:B",
                "PYR",
                "EXC",
                7697.0,
                1306.0,
                9391.0,
            ),
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


def test_assign_placeholders(tmp_path, nodes_file_two_regions):
    output_dir = create_dir(tmp_path / "out")

    morph1_path = output_dir / "foo.swc"
    morph2_path = output_dir / "bar.swc"
    shutil.copy(DATA_DIR / "placeholder_morphology.swc", morph1_path)
    shutil.copy(DATA_DIR / "placeholder_morphology.swc", morph2_path)

    config = {
        "SSp-bfd2": {"L2_TPC:B": [morph1_path]},
        "SSp-bfd3": {"L2_TPC:B": [morph2_path]},
    }
    config_file = output_dir / "config.json"
    write_json(data=config, filepath=config_file)

    out_morphologies_dir = output_dir / "morphologies"
    out_nodes_file = output_dir / "nodes.h5"

    test_module.assign_placeholders(
        nodes_file=nodes_file_two_regions,
        config_file=config_file,
        out_morphologies_dir=out_morphologies_dir,
        out_nodes_file=out_nodes_file,
    )

    old_cells = voxcell.CellCollection.load_sonata(nodes_file_two_regions)
    df_old_cells = old_cells.as_dataframe()

    new_cells = voxcell.CellCollection.load_sonata(out_nodes_file)
    df_new_cells = new_cells.as_dataframe()

    assert old_cells.population_name == new_cells.population_name

    # check output directory populated with both asc and h5 files
    morphology_files = sorted(p.name for p in out_morphologies_dir.iterdir())
    assert morphology_files == [
        "bar.asc",
        "bar.h5",
        "foo.asc",
        "foo.h5",
    ], morphology_files

    # assignment adds columns morphology, morphology_producer, and orientation
    assert set(df_new_cells.columns) == set(df_old_cells.columns) | {
        "morphology",
        "morphology_producer",
        "orientation",
    }

    # producer has to be 'placeholder'
    assert all(df_new_cells.morphology_producer == "placeholder")

    # check names of morphologies assigned
    assert df_new_cells.morphology.tolist() == ["foo", "foo", "bar", "bar"]

    # check unit orientations have been assigned
    assert all(
        new_cells.orientations == np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])
    )


def _write_empty_nodes(ref_file, out_file):
    old_cells = test_module._empty_cell_collection(voxcell.CellCollection.load_sonata(ref_file))
    old_cells.save_sonata(out_file)
    return old_cells


def test_assign_placeholders(tmp_path, nodes_file_one_region):
    output_dir = create_dir(tmp_path / "out")

    # create empty nodes with realistic columns
    nodes_file = Path(tmp_path / "empty.h5")
    old_cells = _write_empty_nodes(nodes_file_one_region, nodes_file)

    out_nodes_file = output_dir / "nodes.h5"

    test_module.assign_placeholders(
        nodes_file=nodes_file,
        config_file=None,
        out_morphologies_dir=None,
        out_nodes_file=out_nodes_file,
    )

    df_old_cells = old_cells.as_dataframe()

    new_cells = voxcell.CellCollection.load_sonata(out_nodes_file)
    df_new_cells = new_cells.as_dataframe()

    assert len(new_cells) == 0
    assert set(df_new_cells.columns) == set(df_old_cells.columns)


def test_merge_cli():
    _check_arg_consistency(test_module.merge_cli, test_module.merge)


TO_MERGE_COLS = []


TO_MERGE_COLS = [
    "morph_class",
    "morphology",
    "morphology_producer",
    "synapse_class",
    "mtype",
    "subregion",
    "etype",
    "hemisphere",
    "region",
    "split_index",
]


@pytest.fixture
def synthesized_nodes_file(tmp_path):
    # fmt: off
    df = pd.DataFrame.from_records(
        [
            ("P0", "e3e706", "synthesis", "EXC", "M0", "R0", "E0", "l", "R0", 0),
            ("P2", "cd613e", "synthesis", "EXC", "M2", "R2", "E2", "r", "R2", 2),
        ],
        columns=TO_MERGE_COLS,
        index=pd.RangeIndex(start=1, stop=3),
    )
    # fmt: on

    cells = voxcell.CellCollection.from_dataframe(df)
    cells.population_name = "foo"

    cells.positions = np.array([[0.0, 0.0, 0.0], [2.0, 2.0, 2.0]])

    I = np.identity(3)
    cells.orientations = np.array([I, I])

    nodes_file = tmp_path / "synthesized_nodes.h5"
    cells.save_sonata(nodes_file)
    return nodes_file


@pytest.fixture
def placeholder_nodes_file(tmp_path):
    df = pd.DataFrame.from_records(
        [
            ("P3", "bar", "placeholder", "EXC", "M3", "R3", "E3", "l", "R3", 3),
            ("P1", "foo", "placeholder", "EXC", "M1", "R1", "E1", "r", "R1", 1),
        ],
        columns=TO_MERGE_COLS,
        index=pd.RangeIndex(start=1, stop=3),
    )
    # fmt: on

    cells = voxcell.CellCollection.from_dataframe(df)
    cells.population_name = "foo"

    cells.positions = np.array([[3.0, 3.0, 3.0], [1.0, 1.0, 1.0]])

    I = np.identity(3)
    cells.orientations = np.array([I, I])

    nodes_file = tmp_path / "placeholder_nodes.h5"
    cells.save_sonata(nodes_file)
    return nodes_file


@pytest.fixture
def empty_nodes_file(tmp_path, placeholder_nodes_file):
    nodes_file = tmp_path / "empty_nodes.h5"
    empty_nodes = _write_empty_nodes(placeholder_nodes_file, nodes_file)
    empty_nodes.properties = empty_nodes.properties.drop(columns="split_index")
    empty_nodes.save_sonata(nodes_file)

    return nodes_file


def test_merge__both_nonempty(tmp_path, synthesized_nodes_file, placeholder_nodes_file):
    out_nodes_file = tmp_path / "nodes.h5"

    test_module.merge(
        synthesized_nodes_file=synthesized_nodes_file,
        placeholder_nodes_file=placeholder_nodes_file,
        out_nodes_file=out_nodes_file,
    )

    cells = voxcell.CellCollection.load_sonata(out_nodes_file)
    assert cells.population_name == "foo", cells.population_name
    assert len(cells) == 4

    # split_index is removed after being used for merging
    assert set(cells.properties.columns) == set(TO_MERGE_COLS) - {"split_index"}

    npt.assert_allclose(
        cells.positions,
        [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0], [3.0, 3.0, 3.0]],
    )

    assert cells.properties.etype.tolist() == ["E0", "E1", "E2", "E3"]
    assert cells.properties.morph_class.tolist() == ["P0", "P1", "P2", "P3"]
    assert cells.properties.hemisphere.tolist() == ["l", "r", "r", "l"]
    assert cells.properties.morphology_producer.tolist() == [
        "synthesis",
        "placeholder",
        "synthesis",
        "placeholder",
    ]
    assert cells.properties.mtype.tolist() == ["M0", "M1", "M2", "M3"]
    assert cells.properties.subregion.tolist() == ["R0", "R1", "R2", "R3"]
    assert cells.properties.region.tolist() == ["R0", "R1", "R2", "R3"]
    assert cells.properties.morphology.tolist() == ["e3e706", "foo", "cd613e", "bar"]


def test_merge__one_empty(
    tmp_path, synthesized_nodes_file, placeholder_nodes_file, empty_nodes_file
):
    out_nodes_file = tmp_path / "out_nodes.h5"

    test_module.merge(
        synthesized_nodes_file=empty_nodes_file,
        placeholder_nodes_file=placeholder_nodes_file,
        out_nodes_file=out_nodes_file,
    )
    assert filecmp.cmp(placeholder_nodes_file, out_nodes_file)

    test_module.merge(
        synthesized_nodes_file=synthesized_nodes_file,
        placeholder_nodes_file=empty_nodes_file,
        out_nodes_file=out_nodes_file,
    )
    assert filecmp.cmp(synthesized_nodes_file, out_nodes_file)


def test_merge__both_empty_raises(tmp_path, empty_nodes_file):
    with pytest.raises(CWLWorkflowError, match="Both canonical and placeholder nodes are empty."):
        test_module.merge(
            synthesized_nodes_file=empty_nodes_file,
            placeholder_nodes_file=empty_nodes_file,
            out_nodes_file=None,
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
                        "root__neurons": {
                            "type": "biophysical",
                            "partial": ["cell-properties"],
                        }
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

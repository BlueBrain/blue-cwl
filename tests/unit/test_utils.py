import os
import filecmp
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
from copy import deepcopy

import voxcell
import libsonata
import tempfile
import pytest
import pandas as pd
from pandas import testing as pdt
from numpy import testing as npt

from cwl_registry import utils as tested
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry import constants


DATA_DIR = Path(__file__).parent / "data"


def test_cwd():
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir).resolve()

        with tested.cwd(tdir):
            assert os.getcwd() == str(tdir)
        assert os.getcwd() == str(cwd)


@pytest.fixture
def io_data():
    return {
        "version": "v2.0",
        "neurons": [
            {
                "density": 100000,
                "region": "@3$",
                "traits": {"layer": 3, "mtype": "L23_MC", "etype": {"bAC": 0.7, "bNAC": 0.3}},
            },
            {
                "density": 100000,
                "region": "@5$",
                "traits": {"layer": 5, "mtype": "L5_TPC:A", "etype": {"cADpyr": 1.0}},
            },
        ],
    }


def test_load_yaml(io_data):
    yaml_file = DATA_DIR / "test_file.yml"
    data = tested.load_yaml(yaml_file)
    assert data == io_data


def test_write_yaml(io_data):
    with tempfile.NamedTemporaryFile(suffix=".yml") as tfile:
        tested.write_yaml(data=io_data, filepath=tfile.name)
        filecmp.cmp(tfile.name, DATA_DIR / "test_file.yml")


def test_load_json(io_data):
    json_file = DATA_DIR / "test_file.json"
    data = tested.load_json(json_file)
    assert data == io_data


def test_write_json(io_data):
    with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
        tested.write_yaml(data=io_data, filepath=tfile.name)
        filecmp.cmp(tfile.name, DATA_DIR / "test_file.json")


def test_run_circuit_build_phase():
    with patch("subprocess.run"):
        tested.run_circuit_build_phase(
            bioname_dir="bioname-dir",
            cluster_config_file="some-path",
            phase="place_cells",
            output_dir=".",
        )


def test_build_manifest__default_parameters():
    res = tested.build_manifest(
        region="my-region",
        atlas_dir="my-atlas-dir",
    )
    assert res == {
        "common": {
            "atlas": "my-atlas-dir",
            "region": "my-region",
            "node_population_name": "my-region_neurons",
            "edge_population_name": "my-region_neurons__chemical_synapse",
            "morph_release": "",
            "synthesis": False,
            "partition": ["left", "right"],
        },
        **constants.DEFAULT_CIRCUIT_BUILD_PARAMETERS,
    }


def test_build_manifest__default_parameters_with_release():
    res = tested.build_manifest(
        region="my-region",
        atlas_dir="my-atlas-dir",
        morphology_release_dir="morph-dir",
    )
    assert res == {
        "common": {
            "atlas": "my-atlas-dir",
            "region": "my-region",
            "node_population_name": "my-region_neurons",
            "edge_population_name": "my-region_neurons__chemical_synapse",
            "morph_release": "morph-dir",
            "synthesis": False,
            "partition": ["left", "right"],
        },
        **constants.DEFAULT_CIRCUIT_BUILD_PARAMETERS,
    }


def test_build_manifest__custom_parameters():
    custom_parameters = {
        "step1": {"a": 1},
        "step2": {"b": 2},
    }

    res = tested.build_manifest(
        region="my-region",
        atlas_dir="my-atlas-dir",
        parameters=custom_parameters,
    )
    assert res == {
        "common": {
            "atlas": "my-atlas-dir",
            "region": "my-region",
            "node_population_name": "my-region_neurons",
            "edge_population_name": "my-region_neurons__chemical_synapse",
            "morph_release": "",
            "synthesis": False,
            "partition": ["left", "right"],
        },
        **custom_parameters,
    }


def teste_add_properties_to_node_population():
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir)

        test_path = tdir / "nodes.h5"

        shutil.copyfile(DATA_DIR / "nodes.h5", test_path)

        property1 = [1, 2, 3]
        property2 = ["a", "a", "b"]
        property3 = ["1", "1", "1"]

        properties = {"p1": property1, "p2": property2, "p3": property3}

        tested.write_node_population_with_properties(
            nodes_file=DATA_DIR / "nodes.h5",
            population_name="default",
            properties=properties,
            output_file=test_path,
        )

        population = voxcell.CellCollection.load_sonata(test_path, population_name="default")

        npt.assert_array_equal(population.properties["p1"], property1)
        npt.assert_array_equal(population.properties["p2"], property2)
        npt.assert_array_equal(population.properties["p3"], property3)


@pytest.fixture
def config_nodes_1():
    return {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "networks": {
            "nodes": [
                {
                    "nodes_file": "some-path",
                    "populations": {
                        "a-pop": {
                            "type": "biophysical",
                            "partial": ["cell-properties"],
                        }
                    },
                }
            ]
        },
    }


@pytest.fixture
def config_nodes_2(config_nodes_1):
    config = deepcopy(config_nodes_1)
    config["networks"]["nodes"].append(
        {
            "nodes_file": "some-path",
            "populations": {
                "b-pop": {
                    "type": "biophysical",
                    "partial": ["cell-properties", "morphologies"],
                    "morphologies_dir": "morph-path",
                }
            },
        }
    )
    return config


def test_get_biophysical_partial_population_from_config(config_nodes_1):
    nodes_file, population_name = tested.get_biophysical_partial_population_from_config(
        config_nodes_1
    )
    assert nodes_file == "some-path"
    assert population_name == "a-pop"


def test_get_biophysical_partial_population_from_config__raises():
    config = {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "networks": {
            "nodes": [
                {
                    "nodes_file": "some-path",
                    "populations": {
                        "a-pop": {
                            "type": "not-biophysical",
                            "partial": ["cell-properties"],
                        }
                    },
                }
            ]
        },
    }
    with pytest.raises(CWLWorkflowError, match="No biophysical population found in config"):
        tested.get_biophysical_partial_population_from_config(config)


@pytest.mark.parametrize("config", ["config_nodes_1", "config_nodes_2"])
def test_update_circuit_config_population__no_population(config, request):
    with pytest.raises(CWLWorkflowError, match="Population name John not in config."):
        tested.update_circuit_config_population(
            request.getfixturevalue(config),
            population_name="John",
            population_data={},
            filepath=None,
        )


def _check_population_changes(
    old_config, new_config, population_name, updated_nodes_file, updated_data
):
    for network_type, network_data in old_config["networks"].items():
        for i, entry in enumerate(network_data):
            new_entry = new_config["networks"][network_type][i]
            old_populations = entry["populations"]
            new_populations = new_entry["populations"]
            if population_name in old_populations:
                assert new_entry["nodes_file"] == updated_nodes_file
                for population_name, population_data in old_populations.items():
                    assert population_name in new_populations
                    new_population_data = new_populations[population_name]
                    for key, value in population_data.items():
                        if key in updated_data:
                            # updated values
                            assert new_population_data[key] == updated_data[key]
                        else:
                            # should remain unchanged
                            assert new_population_data[key] == value
            else:
                # ensure that irrelevant data has remained the same
                assert new_config["networks"][network_type][i] == entry


@pytest.mark.parametrize("config_fixture", ["config_nodes_1", "config_nodes_2"])
def test_update_circuit_config_population__node_population__add_morphologies_dir(
    config_fixture, request
):
    config = request.getfixturevalue(config_fixture)

    res = tested.update_circuit_config_population(
        config,
        population_name="a-pop",
        population_data={
            "partial": ["morphologies"],
            "morphologies_dir": "new-morph-path",
        },
        filepath="new-path",
    )
    _check_population_changes(
        old_config=config,
        new_config=res,
        population_name="a-pop",
        updated_nodes_file="new-path",
        updated_data={
            "partial": ["cell-properties", "morphologies"],
            "morphologies_dir": "new-morph-path",
        },
    )


def test_update_circuit_config_population__node_population__add_emodels__1(config_nodes_1):
    res = tested.update_circuit_config_population(
        config_nodes_1,
        population_name="a-pop",
        population_data={
            "partial": ["emodels"],
            "biophysical_neuron_models_dir": "new-emodels-dir",
        },
        filepath="new-path",
    )
    _check_population_changes(
        old_config=config_nodes_1,
        new_config=res,
        population_name="a-pop",
        updated_nodes_file="new-path",
        updated_data={
            "partial": ["cell-properties", "emodels"],
            "biophysical_models_dir": "new-emodels-dir",
        },
    )


def test_update_circuit_config_population__node_population__add_emodels_2(config_nodes_2):
    res = tested.update_circuit_config_population(
        config_nodes_2,
        population_name="b-pop",
        population_data={
            "partial": ["emodels"],
            "biophysical_neuron_models_dir": "new-emodels-dir",
        },
        filepath="new-path",
    )
    _check_population_changes(
        old_config=config_nodes_2,
        new_config=res,
        population_name="b-pop",
        updated_nodes_file="new-path",
        updated_data={
            "partial": ["cell-properties", "morphologies", "emodels"],
            "biophysical_models_dir": "new-emodels-dir",
        },
    )


def test_update_circuit_config_population__node_population__add_emodels__3(config_nodes_2):
    res = tested.update_circuit_config_population(
        config_nodes_2,
        population_name="a-pop",
        population_data={
            "partial": ["emodels"],
            "biophysical_neuron_models_dir": "new-emodels-dir",
        },
        filepath="new-path",
    )
    _check_population_changes(
        old_config=config_nodes_2,
        new_config=res,
        population_name="a-pop",
        updated_nodes_file="new-path",
        updated_data={
            "partial": ["cell-properties", "emodels"],
            "biophysical_models_dir": "new-emodels-dir",
        },
    )


def test_arrow_io():
    df = pd.DataFrame({"a": [1, 2, 3], "b": pd.Categorical(["a", "b", "b"]), "c": [0.1, 0.2, 0.3]})
    with tempfile.NamedTemporaryFile(suffix=".arrow") as tfile:
        filepath = tfile.name
        tested.write_arrow(filepath=filepath, dataframe=df)
        new_df = tested.load_arrow(filepath=filepath)

    pdt.assert_frame_equal(df, new_df)

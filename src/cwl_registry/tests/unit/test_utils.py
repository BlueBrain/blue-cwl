import shutil
import tempfile
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch

import h5py
import pytest
import voxcell
from numpy import testing as npt

from cwl_registry import constants
from cwl_registry import utils as tested
from cwl_registry.exceptions import CWLWorkflowError

DATA_DIR = Path(__file__).parent / "data"


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


def test_get_config_path_from_circuit_resource__direct_path():

    forge = Mock()
    forge.retrieve.return_value = Mock(circuitConfigPath="file:///some-path")

    path = tested.get_config_path_from_circuit_resource(forge, None)
    assert path == Path("/some-path")

    forge.retrieve.return_value = Mock(circuitConfigPath="/some-path")

    path = tested.get_config_path_from_circuit_resource(forge, None)
    assert path == Path("/some-path")


def test_get_config_path_from_circuit_resource__data_download():

    resource = Mock()
    resource.circuitConfigPath = Mock(type="DataDownload", url="file:///some-path")

    forge = Mock()
    forge.retrieve.return_value = resource

    path = tested.get_config_path_from_circuit_resource(forge, None)
    assert path == Path("/some-path")

    resource.circuitConfigPath = Mock(type="DataDownload", url="/some-path")

    forge = Mock()
    forge.retrieve.return_value = resource

    path = tested.get_config_path_from_circuit_resource(forge, None)
    assert path == Path("/some-path")


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
            "partial": ["cell-properties", "morphologies"],
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


@pytest.mark.parametrize("config_fixture", ["config_nodes_1", "config_nodes_2"])
def test_update_circuit_config_population__node_population__add_emodels(config_fixture, request):

    config = request.getfixturevalue(config_fixture)

    res = tested.update_circuit_config_population(
        config,
        population_name="a-pop",
        population_data={
            "partial": ["cell-properties", "morphologies", "emodels"],
            "biophysical_neuron_models_dir": "new-emodels-dir",
        },
        filepath="new-path",
    )
    _check_population_changes(
        old_config=config,
        new_config=res,
        population_name="a-pop",
        updated_nodes_file="new-path",
        updated_data={
            "partial": ["cell-properties", "morphologies", "emodels"],
            "biophysical_models_dir": "new-emodels-dir",
        },
    )

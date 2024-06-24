import os
from pathlib import Path
from unittest.mock import patch, Mock
import pytest

from blue_cwl.wrappers import memodel as test_module
from blue_cwl.utils import load_json
from blue_cwl.testing import patchenv


DATA_DIR = Path(__file__).parent / "data"


def test_get_biophysical_population_info(circuit_config_file):
    res = test_module._get_biophysical_population_info(circuit_config_file, ext="h5")

    nodes_file, node_population_name, morph_dir = res

    assert nodes_file == "nodes.h5"
    assert node_population_name == "root__neurons"
    assert morph_dir == "morphologies"

    res = test_module._get_biophysical_population_info(circuit_config_file, ext="asc")
    nodes_file, node_population_name, morph_dir = res

    assert nodes_file == "nodes.h5"
    assert node_population_name == "root__neurons"
    assert morph_dir == "morphologies"


def test_stage_circuit(tmp_path, detailed_circuit_metadata, circuit_config_file):
    output_file = tmp_path / "circuit_config.json"

    mock = Mock()
    mock.circuitConfigPath.url = f"file://{circuit_config_file}"

    with patch("blue_cwl.wrappers.memodel.DetailedCircuit.from_id", return_value=mock):
        test_module._stage_circuit(None, output_file)

    res = load_json(output_file)
    assert res == load_json(circuit_config_file)


def test_recipe(tmp_path, materialized_me_model_config_file):
    output_file = tmp_path / "recipe.json"

    test_module.recipe(config_file=materialized_me_model_config_file, output_file=output_file)

    res = load_json(output_file)

    assert res == {
        "library": {
            "eModel": {
                "emodel_8f840b": "AAA__GEN_mtype__GEN_etype__emodel",
                "emodel_23da5a": "AAA__GIN_mtype__GIN_etype__emodel",
                "emodel_371f77": "ACAd1__L1_DAC__bNAC__override",
                "emodel_0ed829": "ACAd1__L1_DAC__cNAC",
            }
        },
        "configuration": {
            "AAA": {
                "GEN_mtype": {
                    "GEN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_8f840b"}
                },
                "GIN_mtype": {
                    "GIN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_23da5a"}
                },
            },
            "ACAd1": {
                "L1_DAC": {
                    "bNAC": {
                        "assignmentAlgorithm": "assignOne",
                        "eModel": "emodel_371f77",
                        "axonInitialSegmentAssignment": {"fixedValue": {"value": 1}},
                    },
                    "cNAC": {"assignmentAlgorithm": "assignOne", "eModel": "emodel_0ed829"},
                }
            },
        },
    }


def test_register(tmp_path, circuit_config_file, circuit_config, detailed_circuit_metadata):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    hoc_dir = output_dir / "hoc"
    hoc_dir.mkdir()

    nodes_file = Path(DATA_DIR / "nodes.h5")

    output_resource_file = output_dir / "resource.json"

    variant = Mock()
    mock = Mock()
    mock.outputBinding = {"glob": "bar.json"}
    variant.tool_definition.outputs = {"foo": mock}

    mock_circuit = Mock()
    mock_circuit.brainLocation.brainRegion.id = "foo"
    mock_circuit.__class__.__name__ = "DetailedCircuit"
    mock_circuit.get_id.return_value = "circuit-id"

    with (
        patch("blue_cwl.wrappers.memodel._register_circuit", return_value=mock_circuit),
    ):
        test_module.register(
            circuit_id="circuit-id",
            nodes_file=nodes_file,
            circuit_file=circuit_config_file,
            hoc_dir=hoc_dir,
            output_dir=output_dir,
            output_resource_file=output_resource_file,
        )

    res_circuit = load_json(output_dir / "circuit_config.json")

    assert res_circuit == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": "node_sets.json",
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(nodes_file),
                    "populations": {
                        "root__neurons": {
                            "type": "biophysical",
                            "partial": ["cell-properties", "morphologies"],
                            "alternate_morphologies": {
                                "h5v1": "morphologies",
                                "neurolucida-asc": "morphologies",
                            },
                            "biophysical_neuron_models_dir": str(hoc_dir),
                        }
                    },
                }
            ],
            "edges": [],
        },
        "metadata": {"status": "partial"},
    }

    assert output_resource_file.exists()
    res_resource = load_json(output_resource_file)
    assert res_resource == {"@id": "circuit-id", "@type": "DetailedCircuit"}

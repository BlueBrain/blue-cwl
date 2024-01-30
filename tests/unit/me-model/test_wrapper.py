import os
from pathlib import Path
from unittest.mock import patch, Mock
import pytest

from cwl_registry.wrappers import memodel as test_module
from cwl_registry.utils import load_json
from cwl_registry.testing import patchenv


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

    with patch("cwl_registry.wrappers.memodel.DetailedCircuit.from_id", return_value=mock):
        test_module._stage_circuit(None, output_file)

    res = load_json(output_file)
    assert res == load_json(circuit_config_file)


def test_build_recipe(tmp_path, materialized_me_model_config_file):
    output_file = tmp_path / "recipe.json"

    test_module._build_recipe(materialized_me_model_config_file, output_file)

    res = load_json(output_file)

    assert res == {
        "library": {
            "eModel": {
                "8f840b": "AAA__GEN_mtype__GEN_etype__emodel",
                "23da5a": "AAA__GIN_mtype__GIN_etype__emodel",
                "371f77": "ACAd1__L1_DAC__bNAC__override",
                "0ed829": "ACAd1__L1_DAC__cNAC",
            }
        },
        "configuration": {
            "AAA": {
                "GEN_mtype": {
                    "GEN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "8f840b"}
                },
                "GIN_mtype": {
                    "GIN_etype": {"assignmentAlgorithm": "assignOne", "eModel": "23da5a"}
                },
            },
            "ACAd1": {
                "L1_DAC": {
                    "bNAC": {
                        "assignmentAlgorithm": "assignOne",
                        "eModel": "371f77",
                        "axonInitialSegmentAssignment": {"fixedValue": {"value": 1}},
                    },
                    "cNAC": {"assignmentAlgorithm": "assignOne", "eModel": "0ed829"},
                }
            },
        },
    }


def test_run_emodel_prepare():
    with (
        patch("subprocess.run") as patched_subprocess,
        patch(
            "cwl_registry.utils.build_variant_allocation_command",
            side_effect=lambda e, *args, **kwargs: e,
        ),
    ):
        test_module._run_emodel_prepare(
            recipe_file="recipe-file",
            mechanisms_dir="mechanisms-dir",
            work_dir="work-dir",
            variant=None,
        )
        expected_command = (
            "emodel-generalisation -v prepare "
            "--config-path recipe-file "
            "--local-config-path work-dir/configs "
            "--mechanisms-path mechanisms-dir"
        )
        patched_subprocess.assert_called_once_with(
            expected_command,
            check=True,
            shell=True,
        )


def test_run_emodel_assign(circuit_config_file):
    with (
        patch("subprocess.run") as patched_subprocess,
        patch("cwl_registry.validation.check_properties_in_population"),
        patch(
            "cwl_registry.utils.build_variant_allocation_command",
            side_effect=lambda e, *args, **kwargs: e,
        ),
    ):
        test_module._run_emodel_assign(
            circuit_config_file=circuit_config_file,
            recipe_file="recipe-file",
            output_nodes_file="out-file",
            variant=None,
            work_dir=Path("work-dir"),
        )
        expected_command = (
            "emodel-generalisation -v --no-progress assign "
            "--input-node-path nodes.h5 "
            "--config-path recipe-file "
            "--output-node-path out-file "
            "--local-config-path work-dir/configs"
        )

        patched_subprocess.assert_called_once_with(
            expected_command,
            check=True,
            shell=True,
        )


@patchenv(foo="bar")
def test_run_emodel_adapt(circuit_config_file):
    with (
        patch("subprocess.run") as patched_subprocess,
        patch("cwl_registry.validation.check_properties_in_population"),
        patch(
            "cwl_registry.utils.build_variant_allocation_command",
            side_effect=lambda e, *args, **kwargs: e,
        ),
    ):
        test_module._run_emodel_adapt(
            circuit_config_file=circuit_config_file,
            nodes_file="nodes-file-path",
            recipe_file="recipe-file",
            output_nodes_file="out-file",
            output_biophysical_models_dir="hoc-dir",
            variant=None,
            work_dir=Path("work-dir"),
            mechanisms_dir="mechanisms-dir",
        )
        expected_command = (
            "emodel-generalisation -v --no-progress adapt "
            "--input-node-path nodes-file-path "
            "--output-node-path out-file "
            "--morphology-path morphologies "
            "--config-path recipe-file "
            "--output-hoc-path hoc-dir "
            "--parallel-lib dask_dataframe "
            "--local-config-path work-dir/configs "
            "--local-dir work-dir/local"
        )
        patched_subprocess.assert_called_once_with(
            expected_command,
            check=True,
            shell=True,
            env={
                "foo": "bar",
                "EMODEL_GENERALISATION_MOD_LIBRARY_PATH": "mechanisms-dir",
                "NEURON_MODULE_OPTIONS": "-nogui",
            },
        )


@patchenv(foo="bar")
def test_run_emodel_currents(circuit_config_file):
    with (
        patch("subprocess.run") as patched_subprocess,
        patch("cwl_registry.validation.check_properties_in_population"),
        patch(
            "cwl_registry.utils.build_variant_allocation_command",
            side_effect=lambda e, *args, **kwargs: e,
        ),
    ):
        test_module._run_emodel_currents(
            circuit_config_file=circuit_config_file,
            nodes_file="nodes-file-path",
            biophysical_neuron_models_dir="hoc-dir",
            output_nodes_file="out-file",
            variant=None,
            mechanisms_dir="mechanisms-dir",
        )
        expected_command = (
            "emodel-generalisation -v --no-progress compute_currents "
            "--input-path nodes-file-path "
            "--output-path out-file "
            "--morphology-path morphologies "
            "--hoc-path hoc-dir "
            "--parallel-lib dask_dataframe"
        )
        patched_subprocess.assert_called_once_with(
            expected_command,
            check=True,
            shell=True,
            env={
                "foo": "bar",
                "EMODEL_GENERALISATION_MOD_LIBRARY_PATH": "mechanisms-dir",
                "NEURON_MODULE_OPTIONS": "-nogui",
            },
        )


def test_register(tmp_path, circuit_config_file, circuit_config, detailed_circuit_metadata):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    output_cicuit_config_file = tmp_path / "circuit_config.json"

    partial_circuit = Mock()
    partial_circuit.brainLocation.brainRegion.id = "foo"
    partial_circuit.atlasRelease.id = "bar"
    partial_circuit.get_id.return_value = "zoo"

    variant = Mock()
    mock = Mock()
    mock.outputBinding = {"glob": "bar.json"}
    variant.tool_definition.outputs = {"foo": mock}

    mock_circuit = Mock()
    mock_circuit.brainLocation.brainRegion.id = "foo"

    with (
        patch("cwl_registry.wrappers.memodel._register_circuit"),
        patch("cwl_registry.wrappers.memodel.load_by_id", return_value=detailed_circuit_metadata),
    ):
        test_module._register(
            partial_circuit=partial_circuit,
            variant=variant,
            circuit_config_file=circuit_config_file,
            nodes_file="new-nodes-file",
            biophysical_neuron_models_dir="hoc-dir",
            output_dir=output_dir,
        )

    res1 = load_json(output_dir / "circuit_config.json")

    assert res1 == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": "node_sets.json",
        "networks": {
            "nodes": [
                {
                    "nodes_file": "new-nodes-file",
                    "populations": {
                        "root__neurons": {
                            "type": "biophysical",
                            "partial": ["cell-properties", "morphologies"],
                            "alternate_morphologies": {
                                "h5v1": "morphologies",
                                "neurolucida-asc": "morphologies",
                            },
                            "biophysical_neuron_models_dir": "hoc-dir",
                        }
                    },
                }
            ],
            "edges": [],
        },
        "metadata": {"status": "partial"},
    }

    res2 = load_json(output_dir / "bar.json")

    assert res2 == detailed_circuit_metadata

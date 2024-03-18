"""me-model wrapper."""

import logging
import os
import shutil
import subprocess
from pathlib import Path

import click
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity, unquote_uri_path

from cwl_registry import registering, utils, validation
from cwl_registry.me_model.entity import MEModelConfig
from cwl_registry.me_model.recipe import build_me_model_recipe
from cwl_registry.me_model.staging import stage_me_model_config
from cwl_registry.nexus import get_distribution_as_dict
from cwl_registry.staging import stage_file
from cwl_registry.utils import get_partial_circuit_region_id
from cwl_registry.variant import Variant

L = logging.getLogger(__name__)


INPUT_POPULATION_COLUMNS = [
    "etype",
    "mtype",
    "region",
    "subregion",
    "morphology",
]


ASSIGN_PROPERTIES = ["model_template"]
ADAPT_PROPERTIES = [
    "dynamics_params/AIS_scaler",
    "dynamics_params/soma_scaler",
]
CURRENTS_PROPERTIES = [
    "dynamics_params/holding_current",
    "dynamics_params/input_resistance",
    "dynamics_params/threshold_current",
    "dynamics_params/resting_potential",
]


@click.group(name="me-model")
def app():
    """me-model app."""


@app.command()
@click.option("--configuration", required=True, help="Configuration id.")
@click.option("--partial-circuit", required=True, help="Partial circuit id.")
@click.option("--variant-config", required=True, help="Variant id.")
@click.option("--output-dir", required=True, help="Output directory path")
def mono_execution(configuration, partial_circuit, variant_config, output_dir):
    """Single execution endpoint."""
    _mono_execution(configuration, partial_circuit, variant_config, output_dir)


def _mono_execution(configuration, partial_circuit, variant_config, output_dir):
    output_dir = utils.create_dir(output_dir)

    variant = get_entity(resource_id=variant_config, cls=Variant)
    staging_dir = utils.create_dir(output_dir / "stage")
    build_dir = utils.create_dir(output_dir / "build")

    configuration_file = staging_dir / "materialized_me_model_config.json"

    stage_me_model_config(
        dataset=get_distribution_as_dict(configuration, cls=MEModelConfig),
        staging_dir=staging_dir / "me-model-config",
        output_file=configuration_file,
    )

    circuit_config_file = staging_dir / "circuit_config.json"
    _stage_circuit(
        partial_circuit=partial_circuit,
        output_file=circuit_config_file,
    )

    recipe_file = build_dir / "recipe.json"
    _build_recipe(configuration_file=configuration_file, output_file=recipe_file)

    mechanisms_dir = utils.create_dir(build_dir / "mechanisms")
    _run_emodel_prepare(
        recipe_file=recipe_file,
        mechanisms_dir=mechanisms_dir,
        variant=variant,
        work_dir=build_dir,
    )

    assign_nodes_file = build_dir / "assign_nodes.h5"
    _run_emodel_assign(
        circuit_config_file=circuit_config_file,
        recipe_file=recipe_file,
        output_nodes_file=assign_nodes_file,
        work_dir=build_dir,
        variant=variant,
    )

    output_biophysical_models_dir = utils.create_dir(build_dir / "hoc")

    adapt_nodes_file = build_dir / "adapt_nodes.h5"
    _run_emodel_adapt(
        circuit_config_file=circuit_config_file,
        nodes_file=assign_nodes_file,
        recipe_file=recipe_file,
        output_nodes_file=adapt_nodes_file,
        output_biophysical_models_dir=output_biophysical_models_dir,
        variant=variant,
        work_dir=build_dir,
        mechanisms_dir=mechanisms_dir,
    )

    output_nodes_file = build_dir / "nodes.h5"
    _run_emodel_currents(
        circuit_config_file=circuit_config_file,
        nodes_file=adapt_nodes_file,
        biophysical_neuron_models_dir=output_biophysical_models_dir,
        output_nodes_file=output_nodes_file,
        variant=variant,
        mechanisms_dir=mechanisms_dir,
    )

    _register(
        partial_circuit=partial_circuit,
        variant=variant,
        circuit_config_file=circuit_config_file,
        nodes_file=output_nodes_file,
        biophysical_neuron_models_dir=output_biophysical_models_dir,
        output_dir=output_dir,
    )


def _rmdir_if_exists(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    return path


def _stage_circuit(partial_circuit, output_file):
    entity = get_entity(resource_id=partial_circuit, cls=DetailedCircuit)
    stage_file(
        source=unquote_uri_path(entity.circuitConfigPath.url),
        target=output_file,
        symbolic=True,
    )


def _build_recipe(configuration_file, output_file):
    configuration = utils.load_json(configuration_file)
    recipe = build_me_model_recipe(me_model_config=configuration)
    utils.write_json(data=recipe, filepath=output_file)


def _run_emodel_prepare(recipe_file, mechanisms_dir, variant, work_dir):
    local_config_path = _rmdir_if_exists(Path(work_dir, "configs"))

    arglist = [
        "emodel-generalisation",
        "-v",
        "prepare",
        "--config-path",
        str(recipe_file),
        "--local-config-path",
        str(local_config_path),
        "--mechanisms-path",
        str(mechanisms_dir),
    ]

    cmd = " ".join(arglist)
    cmd = utils.build_variant_allocation_command(cmd, variant, sub_task_index=0)

    env = os.environ | {"NEURON_MODULE_OPTIONS": "-nogui"}

    L.info("Tool command :%s", cmd)
    subprocess.run(cmd, check=True, shell=True, env=env)


def _run_emodel_assign(circuit_config_file, recipe_file, output_nodes_file, work_dir, variant):
    nodes_file, population_name, _ = _get_biophysical_population_info(
        circuit_config_file=circuit_config_file,
        ext="asc",
    )
    validation.check_properties_in_population(population_name, nodes_file, INPUT_POPULATION_COLUMNS)

    arglist = [
        "emodel-generalisation",
        "-v",
        "--no-progress",
        "assign",
        "--input-node-path",
        str(nodes_file),
        "--config-path",
        str(recipe_file),
        "--output-node-path",
        str(output_nodes_file),
        "--local-config-path",
        str(Path(work_dir, "configs")),
    ]

    cmd = " ".join(arglist)
    cmd = utils.build_variant_allocation_command(cmd, variant, sub_task_index=0)

    env = os.environ | {"NEURON_MODULE_OPTIONS": "-nogui"}

    L.info("Tool command :%s", cmd)
    subprocess.run(cmd, check=True, shell=True, env=env)

    L.info("Validating generated nodes file...")
    validation.check_properties_in_population(
        population_name=population_name,
        nodes_file=output_nodes_file,
        property_names=INPUT_POPULATION_COLUMNS + ASSIGN_PROPERTIES,
    )


def _run_emodel_adapt(
    circuit_config_file,
    nodes_file,
    recipe_file,
    output_nodes_file,
    output_biophysical_models_dir,
    variant,
    work_dir,
    mechanisms_dir,
):
    _, population_name, morphologies_dir = _get_biophysical_population_info(
        circuit_config_file=circuit_config_file,
        ext="asc",
    )

    local_dir = _rmdir_if_exists(work_dir / "local")

    arglist = [
        "emodel-generalisation",
        "-v",
        "--no-progress",
        "adapt",
        "--input-node-path",
        str(nodes_file),
        "--output-node-path",
        str(output_nodes_file),
        "--morphology-path",
        str(morphologies_dir),
        "--config-path",
        str(recipe_file),
        "--output-hoc-path",
        str(output_biophysical_models_dir),
        "--parallel-lib",
        _parallel_mode(),
        "--local-config-path",
        str(work_dir / "configs"),
        "--local-dir",
        str(local_dir),
    ]
    cmd = " ".join(arglist)
    cmd = utils.build_variant_allocation_command(cmd, variant, sub_task_index=1)

    L.info("Tool command :%s", cmd)

    env = os.environ.copy()
    env.update(
        {
            "EMODEL_GENERALISATION_MOD_LIBRARY_PATH": str(mechanisms_dir),
            "NEURON_MODULE_OPTIONS": "-nogui",
        }
    )
    subprocess.run(cmd, check=True, shell=True, env=env)

    L.info("Validating generated nodes file...")
    validation.check_properties_in_population(
        population_name=population_name,
        nodes_file=output_nodes_file,
        property_names=INPUT_POPULATION_COLUMNS + ASSIGN_PROPERTIES + ADAPT_PROPERTIES,
    )


def _run_emodel_currents(
    circuit_config_file,
    nodes_file,
    biophysical_neuron_models_dir,
    output_nodes_file,
    variant,
    mechanisms_dir,
):
    _, population_name, morphologies_dir = _get_biophysical_population_info(
        circuit_config_file=circuit_config_file,
        ext="asc",
    )
    arglist = [
        "emodel-generalisation",
        "-v",
        "--no-progress",
        "compute_currents",
        "--input-path",
        str(nodes_file),
        "--output-path",
        str(output_nodes_file),
        "--morphology-path",
        str(morphologies_dir),
        "--hoc-path",
        str(biophysical_neuron_models_dir),
        "--parallel-lib",
        _parallel_mode(),
    ]
    cmd = " ".join(arglist)
    cmd = utils.build_variant_allocation_command(cmd, variant, sub_task_index=2)

    L.info("Tool command :%s", cmd)
    env = os.environ.copy()
    env.update(
        {
            "EMODEL_GENERALISATION_MOD_LIBRARY_PATH": str(mechanisms_dir),
            "NEURON_MODULE_OPTIONS": "-nogui",
        }
    )
    subprocess.run(cmd, check=True, shell=True, env=env)

    L.info("Validating generated nodes file...")
    validation.check_properties_in_population(
        population_name=population_name,
        nodes_file=output_nodes_file,
        property_names=INPUT_POPULATION_COLUMNS
        + ASSIGN_PROPERTIES
        + ADAPT_PROPERTIES
        + CURRENTS_PROPERTIES,
    )


def _get_biophysical_population_info(circuit_config_file, ext):
    config = utils.load_json(circuit_config_file)
    nodes_file, node_population_name = utils.get_biophysical_partial_population_from_config(config)
    morphologies_dir = utils.get_morphologies_dir(
        circuit_config=config,
        population_name=node_population_name,
        ext=ext,
    )
    return nodes_file, node_population_name, morphologies_dir


def _register(
    partial_circuit,
    variant,
    circuit_config_file,
    nodes_file,
    biophysical_neuron_models_dir,
    output_dir,
):
    config = utils.load_json(circuit_config_file)

    _, population_name = utils.get_biophysical_partial_population_from_config(config)

    updated_config = utils.update_circuit_config_population(
        config=config,
        population_name=population_name,
        population_data={
            "biophysical_neuron_models_dir": str(biophysical_neuron_models_dir),
        },
        filepath=str(nodes_file),
    )
    output_circuit_config_file = output_dir / "circuit_config.json"
    utils.write_json(filepath=output_circuit_config_file, data=updated_config)
    L.info("Created circuit config file at %s", output_circuit_config_file)

    validation.check_population_name_in_config(population_name, output_circuit_config_file)

    circuit = _register_circuit(partial_circuit, output_circuit_config_file)

    utils.write_resource_to_definition_output(
        json_resource=load_by_id(circuit.get_id()),
        variant=variant,
        output_dir=output_dir,
    )


def _register_circuit(partial_circuit, output_circuit_config_file):
    partial_circuit = get_entity(resource_id=partial_circuit, cls=DetailedCircuit)

    circuit = registering.register_partial_circuit(
        name="Partial circuit with biohysical emodel properties",
        brain_region_id=get_partial_circuit_region_id(partial_circuit),
        atlas_release_id=partial_circuit.atlasRelease.get_id(),
        description="Partial circuit with cell properties, morphologies, and biophysical models.",
        sonata_config_path=output_circuit_config_file,
    )

    return circuit


def _parallel_mode():
    mode = os.getenv("ME_MODEL_MULTIPROCESSING_BACKEND", "dask_dataframe")
    L.debug("Multiprocessing backend: %s", mode)
    return mode

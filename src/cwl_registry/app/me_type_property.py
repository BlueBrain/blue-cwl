"""Morphoelectrical type generator function module."""
import logging
import subprocess
from pathlib import Path
from typing import Any, Dict

import click
import voxcell

from cwl_registry import Variant, recipes, registering, staging, utils
from cwl_registry.hashing import get_target_hexdigest
from cwl_registry.nexus import get_forge

STAGE_DIR_NAME = "stage"
TRANSFORM_DIR_NAME = "transform"
EXECUTE_DIR_NAME = "build"


L = logging.getLogger(__name__)


@click.command()
@click.option("--region", required=True)
@click.option("--variant-config", required=False)
@click.option("--me-type-densities", required=True)
@click.option("--atlas", required=True)
@click.option("--nexus-base", required=True)
@click.option("--nexus-project", required=True)
@click.option("--nexus-org", required=True)
@click.option("--nexus-token", required=True)
@click.option("--task-digest", required=True)
@click.option("--output-dir", required=True)
def app(
    region,
    variant_config,
    me_type_densities,
    atlas,
    nexus_base,
    nexus_project,
    nexus_org,
    nexus_token,
    task_digest,
    output_dir,
):
    """Morphoelectrical type generator cli entry."""
    output_dir = utils.create_dir(Path(output_dir).resolve())

    staged_entities = _extract(
        region,
        variant_config,
        me_type_densities,
        atlas,
        output_dir,
        nexus_base,
        nexus_token,
        nexus_org,
        nexus_project,
    )

    transform_dir = utils.create_dir(output_dir / TRANSFORM_DIR_NAME)
    transformed_entities = _transform(staged_entities, output_dir=transform_dir)

    generated_entities = _generate(transformed_entities, output_dir)

    _register(
        region,
        generated_entities,
        nexus_base,
        nexus_token,
        nexus_org,
        nexus_project,
        task_digest,
    )


def _extract(
    brain_region_id: str,
    variant_config_id: str,
    me_type_densities_id: str,
    atlas_id: str,
    output_dir: Path,
    nexus_base: str,
    nexus_token: str,
    nexus_org: str,
    nexus_project: str,
) -> Dict[str, Any]:
    """Stage resources from the knowledge graph."""
    staging_dir = utils.create_dir(output_dir / STAGE_DIR_NAME)
    variant_dir = utils.create_dir(staging_dir / "variant")
    atlas_dir = utils.create_dir(staging_dir / "atlas")
    me_type_densities_file = staging_dir / "mtype-densities.json"

    forge = get_forge(
        nexus_base=nexus_base,
        nexus_org=nexus_org,
        nexus_project=nexus_project,
        nexus_token=nexus_token,
    )
    variant = Variant.from_resource_id(forge, variant_config_id, staging_dir=variant_dir)

    region = forge.retrieve(brain_region_id, cross_bucket=True).notation

    staging.stage_atlas(
        forge=forge,
        resource_id=atlas_id,
        output_dir=atlas_dir,
        parcellation_ontology_basename="hierarchy.json",
        parcellation_volume_basename="brain_regions.nrrd",
    )

    staging.stage_me_type_densities(
        forge=forge,
        resource_id=me_type_densities_id,
        output_file=me_type_densities_file,
    )

    return {
        "region": region,
        "atlas-dir": atlas_dir,
        "me-type-densities-file": me_type_densities_file,
        "variant": variant,
    }


def _transform(staged_data: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    """Trasform the staged resources into the algorithm's inputs, if needed."""
    region = staged_data["region"]
    variant = staged_data["variant"]

    me_type_densities = utils.load_json(staged_data["me-type-densities-file"])

    composition_file = output_dir / "cell_composition.yaml"
    composition = recipes.build_cell_composition_from_me_densities(region, me_type_densities)
    utils.write_yaml(composition_file, composition)

    mtypes = [me_type_densities[identifier]["label"] for identifier in me_type_densities]

    mtype_taxonomy_file = output_dir / "mtype_taxonomy.tsv"
    mtype_taxonomy = recipes.build_mtype_taxonomy(mtypes)
    mtype_taxonomy.to_csv(mtype_taxonomy_file, sep=" ", index=False)

    return {
        "region": region,
        "atlas-dir": staged_data["atlas-dir"],
        "parameters": utils.load_yaml(variant.get_config_file("parameters.yml"))["place_cells"],
        "composition-file": composition_file,
        "mtype-taxonomy-file": mtype_taxonomy_file,
        "cluster-config": staged_data["variant"].get_resources_file("cluster_config.yml"),
    }


def _generate(transformed_data: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    """Generation step where the algorithm is executed and outputs are created."""
    build_dir = utils.create_dir(output_dir / "build")

    region = transformed_data["region"]
    parameters = transformed_data["parameters"]

    nodes_file = build_dir / "nodes.h5"
    node_population_name = f"{region}__neurons"

    init_cells_file = build_dir / "init_nodes.h5"
    cells = voxcell.CellCollection(node_population_name)
    cells.save(init_cells_file)

    cmd = list(
        map(
            str,
            (
                "brainbuilder",
                "cells",
                "place",
                "--composition",
                transformed_data["composition-file"],
                "--mtype-taxonomy",
                transformed_data["mtype-taxonomy-file"],
                "--atlas",
                transformed_data["atlas-dir"],
                "--atlas-cache",
                output_dir / ".atlas",
                "--region",
                region,
                "--soma-placement",
                parameters["soma_placement"],
                "--density-factor",
                parameters["density_factor"],
                "--atlas-property",
                "region ~brain_regions",
                "--sort-by",
                ",".join(parameters["sort_by"]),
                "--seed",
                parameters["seed"],
                "--output",
                nodes_file,
                "--input",
                init_cells_file,
            ),
        )
    )
    str_command = " ".join(cmd)
    L.debug("Command: %s", str_command)
    subprocess.run(
        str_command,
        check=True,
        capture_output=False,
        shell=True,
    )
    data = {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(nodes_file),
                    "populations": {
                        node_population_name: {
                            "type": "biophysical",
                            "partial": ["cell-properties"],
                        }
                    },
                }
            ]
        },
        "metadata": {"status": "partial"},
    }
    sonata_config_file = build_dir / "config.json"
    utils.write_json(filepath=sonata_config_file, data=data)

    return {
        "partial-circuit": sonata_config_file,
    }


def _register(
    region_id,
    generated_data,
    nexus_base,
    nexus_token,
    nexus_org,
    nexus_project,
    task_digest,
):
    """Register outputs to nexus."""
    forge = get_forge(
        nexus_base=nexus_base,
        nexus_org=nexus_org,
        nexus_project=nexus_project,
        nexus_token=nexus_token,
    )
    target_digest = get_target_hexdigest(
        task_digest,
        "circuit_me_type_bundle",
    )
    registering.register_partial_circuit(
        forge,
        name="Cell properties partial circuit",
        brain_region=region_id,
        description="Partial circuit built with cell positions and me properties.",
        sonata_config_path=generated_data["partial-circuit"],
        target_digest=target_digest,
    )

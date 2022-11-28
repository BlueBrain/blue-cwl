"""Morphoelectrical type generator function module."""
from pathlib import Path
from typing import Any, Dict

import click

from cwl_registry import Variant, recipes, registering, staging, utils
from cwl_registry.hashing import get_target_hexdigest
from cwl_registry.nexus import get_forge

STAGE_DIR_NAME = "stage"
TRANSFORM_DIR_NAME = "transform"
EXECUTE_DIR_NAME = "build"


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

    region = forge.retrieve(brain_region_id, cross_bucket=True).label

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
    bioname_dir = utils.create_dir(output_dir / "bioname")

    region = staged_data["region"]
    variant = staged_data["variant"]

    manifest = utils.build_manifest(
        region=region,
        atlas_dir=staged_data["atlas-dir"],
        parameters=utils.load_yaml(variant.get_config_file("parameters.yml")),
    )
    utils.write_yaml(filepath=bioname_dir / "MANIFEST.yaml", data=manifest)

    me_type_densities = utils.load_json(staged_data["me-type-densities-file"])

    composition = recipes.build_cell_composition_from_me_densities(region, me_type_densities)
    utils.write_yaml(bioname_dir / "cell_composition.yaml", composition)

    mtypes = [me_type_densities[identifier]["label"] for identifier in me_type_densities]
    mtype_taxonomy = recipes.build_mtype_taxonomy(mtypes)
    mtype_taxonomy.to_csv(bioname_dir / "mtype_taxonomy.tsv", sep=" ", index=False)

    return {
        "bioname-dir": bioname_dir,
        "manifest": manifest,
        "cluster-config": staged_data["variant"].get_resources_file("cluster_config.yml"),
    }


def _generate(transformed_data: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    """Generation step where the algorithm is executed and outputs are created."""
    build_dir = utils.create_dir(output_dir / "build")

    utils.run_circuit_build_phase(
        bioname_dir=transformed_data["bioname-dir"],
        cluster_config_file=transformed_data["cluster-config"],
        phase="place_cells",
        output_dir=build_dir,
    )

    # write sonata circuit config for the partial circuit
    manifest = transformed_data["manifest"]
    node_population_name = manifest["common"]["node_population_name"]

    data = {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(build_dir / "auxiliary/circuit.somata.h5"),
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

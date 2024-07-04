import filecmp
from pathlib import Path
from unittest.mock import patch

import pytest
import voxcell
import pandas as pd
import pandas.testing as pdt

from blue_cwl import staging
from blue_cwl.testing import check_arg_consistency
from blue_cwl.wrappers import neurons_cell_position as test_module
from blue_cwl.utils import load_json, load_text, load_yaml, write_yaml, write_json

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def expected_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("expected")


@pytest.mark.parametrize(
    "command, function",
    [
        (test_module.stage_cli, test_module.stage),
        (test_module.transform_cli, test_module.transform),
        (test_module.build_cli, test_module.build),
        (test_module.register_cli, test_module.register),
    ],
)
def test_cli_arguments(command, function):
    check_arg_consistency(command, function)


@pytest.fixture(scope="session")
def region_file(expected_dir):
    """Expected region file."""
    filepath = expected_dir / "region.txt"
    filepath.write_text("root")
    return filepath


@pytest.fixture(scope="session")
def atlas_file(
    expected_dir,
    parcellation_ontology_distribution_file,
    parcellation_volume_distribution_file,
    hemisphere_volume_distribution_file,
):
    atlas_dir = expected_dir / "atlas"
    atlas_dir.mkdir()

    filepath = expected_dir / "atlas.json"

    ontology_file = atlas_dir / "hierarchy.json"
    annotation_file = atlas_dir / "brain_regions.nrrd"
    hemisphere_file = atlas_dir / "hemisphere.nrrd"

    staging.stage_file(source=parcellation_ontology_distribution_file, target=ontology_file)
    staging.stage_file(source=parcellation_volume_distribution_file, target=annotation_file)
    staging.stage_file(source=hemisphere_volume_distribution_file, target=hemisphere_file)
    staging.AtlasInfo(
        ontology_path=str(ontology_file),
        annotation_path=str(annotation_file),
        hemisphere_path=str(hemisphere_file),
        ph_catalog=None,
        cell_orientation_field_path=None,
        directory=str(atlas_dir),
    ).to_file(filepath)
    return filepath


@pytest.fixture(scope="session")
def config_file(expected_dir):
    filepath = expected_dir / "config.json"

    write_json(
        data={
            "place_cells": {
                "soma_placement": "basic",
                "density_factor": 1,
                "sort_by": ["region", "mtype"],
                "seed": 0,
                "mini_frequencies": False,
            },
        },
        filepath=filepath,
    )
    return filepath


@pytest.fixture(scope="session")
def densities_file(expected_dir, me_type_density_distribution_file):
    filepath = expected_dir / "densities.parquet"
    df = pd.DataFrame(
        {
            "mtype": ["L2_TPC:B"],
            "etype": ["cADpyr"],
            "mtype_url": ["http://uri.interlex.org/base/ilx_0381367"],
            "etype_url": ["http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr"],
            "path": [str(me_type_density_distribution_file)],
        }
    )
    df.to_parquet(filepath)

    return filepath


def test_stage(
    tmp_path,
    patch_nexus_calls,
    region_id,
    cell_composition_id,
    config_cell_position_id,
    me_type_density_distribution_file,
    region_file,
    atlas_file,
    config_file,
    densities_file,
):
    stage_dir = tmp_path

    test_module.stage(
        region_id=region_id,
        cell_composition_id=cell_composition_id,
        configuration_id=config_cell_position_id,
        stage_dir=stage_dir,
    )

    res_atlas_info = staging.AtlasInfo.from_file(stage_dir / "atlas.json")
    assert Path(res_atlas_info.ontology_path).exists()
    assert Path(res_atlas_info.annotation_path).exists()
    assert Path(res_atlas_info.directory) == stage_dir / "atlas"

    exp_atlas_info = staging.AtlasInfo.from_file(atlas_file)
    res_hierarchy = load_json(res_atlas_info.ontology_path)
    exp_hierarchy = load_json(exp_atlas_info.ontology_path)
    assert res_hierarchy == exp_hierarchy
    assert filecmp.cmp(res_atlas_info.annotation_path, exp_atlas_info.annotation_path)

    assert load_text(stage_dir / "region.txt") == load_text(region_file)

    res_densities = pd.read_parquet(stage_dir / "densities.parquet")
    exp_densities = pd.read_parquet(densities_file)
    pdt.assert_frame_equal(res_densities, exp_densities)

    res_config = load_json(stage_dir / "config.json")
    exp_config = load_json(config_file)


@pytest.fixture(scope="session")
def mtype_composition_file(expected_dir, me_type_density_distribution_file):
    filepath = expected_dir / "mtype_composition.yml"
    write_yaml(
        data={
            "version": "v2",
            "neurons": [
                {
                    "density": str(me_type_density_distribution_file),
                    "region": "root",
                    "traits": {"mtype": "L2_TPC:B", "etype": "cADpyr"},
                }
            ],
        },
        filepath=filepath,
    )
    return filepath


@pytest.fixture(scope="session")
def mtype_taxonomy_file(expected_dir):
    filepath = expected_dir / "mtype_taxonomy.tsv"
    df = pd.DataFrame(
        {
            "mtype": ["L2_TPC:B"],
            "mClass": ["PYR"],
            "sClass": ["EXC"],
        }
    )
    df.to_csv(filepath, sep=" ", index=False)
    return filepath


def test_transform(
    tmp_path,
    region_file,
    densities_file,
    mtype_composition_file,
    mtype_taxonomy_file,
    me_type_density_distribution_file,
):
    transform_dir = tmp_path

    test_module.transform(
        transform_dir=transform_dir,
        region_file=region_file,
        densities_file=densities_file,
    )

    res_mtype_composition = load_yaml(transform_dir / "mtype_composition.yml")
    exp_mtype_composition = load_yaml(mtype_composition_file)
    assert res_mtype_composition == exp_mtype_composition

    res_mtype_taxonomy = pd.read_csv(transform_dir / "mtype_taxonomy.tsv")
    exp_mtype_taxonomy = pd.read_csv(mtype_taxonomy_file)
    pdt.assert_frame_equal(res_mtype_taxonomy, exp_mtype_taxonomy)


@pytest.fixture(scope="session")
def nodes_file(region_file):
    return DATA_DIR / "cell_positions_nodes.h5"


@pytest.fixture(scope="session")
def node_sets_file(expected_dir):
    filepath = expected_dir / "node_sets.json"
    write_json(
        data={
            "All": {"population": "root__neurons"},
            "Excitatory": {"synapse_class": "EXC"},
            "Inhibitory": {"synapse_class": "INH"},
            "L2_TPC:B": {"mtype": "L2_TPC:B"},
            "cADpyr": {"etype": "cADpyr"},
            "SSp-bfd2": {"region": "SSp-bfd2"},
            "SSp-bfd2/3": ["SSp-bfd2"],
            "SSp-bfd": ["SSp-bfd2/3"],
            "SSp": ["SSp-bfd"],
            "SS": ["SSp"],
            "Isocortex": ["SS"],
            "CTXpl": ["Isocortex"],
            "CTX": ["CTXpl"],
            "CH": ["CTX"],
            "grey": ["CH"],
            "root": ["grey"],
        },
        filepath=filepath,
    )
    return filepath


@pytest.fixture(scope="session")
def summary_file(expected_dir):
    filepath = expected_dir / "summary.json"
    write_json(
        data={
            "version": 1,
            "unitCode": {"density": "mm^-3"},
            "hasPart": {
                "http://api.brain-map.org/api/v2/data/Structure/614454292": {
                    "label": "Primary somatosensory area, barrel field, layer 2",
                    "notation": "SSp-bfd2",
                    "about": "BrainRegion",
                    "hasPart": {
                        "http://uri.interlex.org/base/ilx_0381367": {
                            "label": "L2_TPC:B",
                            "about": "MType",
                            "hasPart": {
                                "http://bbp.epfl.ch/neurosciencegraph/ontologies/etypes/cADpyr": {
                                    "label": "cADpyr",
                                    "about": "EType",
                                    "composition": {
                                        "neuron": {
                                            "density": 203.80542950402037,
                                            "count": 40,
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
            },
        },
        filepath=filepath,
    )
    return filepath


@pytest.fixture(scope="session")
def circuit_file(expected_dir, nodes_file, node_sets_file):
    filepath = expected_dir / "circuit_config.json"
    write_json(
        data={
            "version": 2,
            "manifest": {"$BASE_DIR": "."},
            "node_sets_file": str(node_sets_file),
            "networks": {
                "nodes": [
                    {
                        "nodes_file": str(nodes_file),
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
        },
        filepath=filepath,
    )
    return filepath


def test_build(
    tmp_path,
    atlas_file,
    region_file,
    mtype_composition_file,
    mtype_taxonomy_file,
    config_file,
    densities_file,
    nodes_file,
    node_sets_file,
    summary_file,
    circuit_file,
):
    build_dir = tmp_path

    test_module.build(
        build_dir=build_dir,
        atlas_file=atlas_file,
        region_file=region_file,
        densities_file=densities_file,
        configuration_file=config_file,
        composition_file=mtype_composition_file,
        mtype_taxonomy_file=mtype_taxonomy_file,
    )

    res_cells = voxcell.CellCollection.load_sonata(build_dir / "nodes.h5")
    exp_cells = voxcell.CellCollection.load_sonata(nodes_file)
    assert res_cells.population_name == exp_cells.population_name
    pdt.assert_frame_equal(res_cells.as_dataframe(), exp_cells.as_dataframe())

    res_node_sets = load_json(build_dir / "node_sets.json")
    exp_node_sets = load_json(node_sets_file)
    assert res_node_sets == exp_node_sets

    res_circuit_config = load_json(build_dir / "circuit_config.json")
    assert res_circuit_config == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "node_sets_file": str(build_dir / "node_sets.json"),
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(build_dir / "nodes.h5"),
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

    res_summary = load_json(build_dir / "cell_composition_summary.json")
    exp_summary = load_json(summary_file)
    assert res_summary == exp_summary


def test_register(
    tmp_path,
    patch_nexus_calls,
    region_id,
    cell_composition_id,
    circuit_file,
    summary_file,
):
    output_dir = tmp_path
    output_file = tmp_path / "resource.json"

    test_module.register(
        region_id=region_id,
        cell_composition_id=cell_composition_id,
        circuit_file=circuit_file,
        summary_file=summary_file,
        output_dir=output_dir,
        output_resource_file=output_file,
    )

    resource = load_json(output_file)
    assert resource == {"@id": "detailedcircuit-id", "@type": "DetailedCircuit"}

    summary_resource = load_json(output_dir / "summary_resource.json")
    assert summary_resource == {
        "@id": "cellcompositionsummary-id",
        "@type": "CellCompositionSummary",
    }

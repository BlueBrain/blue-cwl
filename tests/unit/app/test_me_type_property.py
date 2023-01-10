import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from cwl_registry import Variant
import pandas as pd
from cwl_registry.app import me_type_property as tested
from cwl_registry.utils import create_dir, write_yaml, write_json, load_yaml, load_json
from kgforge.core import Resource

from cwl_registry import utils

from tests.unit.mocking import LocalForge


DATA_DIR = Path(__file__).parent.parent / "data"


def _mock_variant(variant_dir):

    config_path = variant_dir / "parameters.yml"
    write_yaml(
        data={
            "place_cells": {
                "step1": {"a": 1},
                "step2": {"b": 2},
            }
        },
        filepath=config_path,
    )
    configs = {"parameters.yml": config_path}
    resources = {"cluster_config.yml": "cluster-config-path"}

    return Variant(
        name="neurons_me_type_property",
        generator_name="me_type_property",
        version="0.1",
        configs=configs,
        resources=resources,
        definitions={},
    )


def _create_me_type_densities(output_file):
    dataset = {
        "http://uri.interlex.org/base/ilx_0383198": {
            "label": "L23_BP",
            "etypes": {
                "http://uri.interlex.org/base/ilx_0738202": {
                    "label": "dSTUT",
                    "path": "L23_BP-DSTUT_densities_v3.nrrd",
                },
                "http://uri.interlex.org/base/ilx_0738206": {
                    "label": "bIR",
                    "path": "L23_BP-BIR_densities_v3.nrrd",
                },
            },
        },
        "http://uri.interlex.org/base/ilx_0383201": {
            "label": "L23_DBC",
            "etypes": {
                "http://uri.interlex.org/base/ilx_0738206": {
                    "label": "bIR",
                    "path": "L23_DBC-BIR_densities_v3.nrrd",
                },
            },
        },
    }
    write_json(filepath=output_file, data={"mtypes": dataset})


def _mock_me_type_densities_resource(resource_dir):

    out_dir = create_dir(resource_dir / "me-type-densities")

    output_file = out_dir / "me_type_densities.json"

    dataset = {
        "hasPart": [
            {
                "@id": "http://uri.interlex.org/base/ilx_0383198",
                "label": "L23_BP",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://uri.interlex.org/base/ilx_0738202",
                        "label": "dSTUT",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@id": "L23_BP-dSTUT-id",
                                "@type": [
                                    "NeuronDensity",
                                    "VolumetricDataLayer",
                                    "CellDensityDataLayer",
                                    "METypeDensity",
                                ],
                                "_rev": 5,
                            }
                        ],
                    },
                    {
                        "@id": "http://uri.interlex.org/base/ilx_0738206",
                        "label": "bIR",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@id": "L23_BP-bIR-id",
                                "@type": [
                                    "NeuronDensity",
                                    "VolumetricDataLayer",
                                    "CellDensityDataLayer",
                                    "METypeDensity",
                                ],
                                "_rev": 5,
                            }
                        ],
                    },
                ],
            },
            {
                "@id": "http://uri.interlex.org/base/ilx_0383201",
                "label": "L23_DBC",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://uri.interlex.org/base/ilx_0738206",
                        "label": "bIR",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@id": "L23_DBC-bIR-id",
                                "@type": [
                                    "NeuronDensity",
                                    "VolumetricDataLayer",
                                    "CellDensityDataLayer",
                                    "METypeDensity",
                                ],
                                "_rev": 5,
                            }
                        ],
                    }
                ],
            },
        ]
    }
    write_json(filepath=output_file, data=dataset)

    info = {}
    info["me-type-densities-id"] = Resource(
        id="me-type-densities-id",
        distribution=Resource(
            type="DataDownload",
            atLocation=Mock(
                location=f"file://{output_file}",
            ),
            encodingFormat="application/json",
            name="me_type_densities.json",
        ),
    )

    for dset in ["L23_BP-dSTUT-id.nrrd", "L23_BP-bIR-id.nrrd", "L23_DBC-bIR-id.nrrd"]:

        path = out_dir / dset
        path.touch()
        info[path.stem] = Resource(
            distribution=Resource(
                type="DataDownload",
                atLocation=Mock(
                    location=f"file://{path}",
                ),
                encodingFormat="application/nrrd",
                name=path.name,
            )
        )
    return info


def _mock_cell_composition_resource(resource_dir):
    densities = _mock_me_type_densities_resource(resource_dir)
    return {
        "cell-composition-id": Resource(
            id="me-type-densities-id",
            cellCompositionVolume=densities["me-type-densities-id"],
        ),
        **densities,
    }


def _mock_atlas_resource(resource_dir):

    atlas = Mock(
        type=["BrainAtlasRelease"],
        parcellationOntology=Mock(id="parcellation-ontology"),
        parcellationVolume=Mock(id="parcellation-volume"),
    )
    volume_path = resource_dir / "mock-volume.nrrd"
    volume_path.touch()
    parcellation_volume = Mock(
        distribution=Resource(
            type="DataDownload",
            atLocation=Mock(
                location=f"file://{volume_path}",
            ),
            encodingFormat="application/nrrd",
            name=volume_path.name,
        )
    )
    hierarchy_path = resource_dir / "mock-hierarchy.json"
    hierarchy_path.touch()
    parcellation_ontology = Mock(
        distribution=Resource(
            type="DataDownload",
            atLocation=Mock(
                location=f"file://{hierarchy_path}",
            ),
            encodingFormat="application/json",
            name=hierarchy_path.name,
        )
    )
    return {
        "atlas-id": atlas,
        "parcellation-ontology": parcellation_ontology,
        "parcellation-volume": parcellation_volume,
    }


def test_me_type_property__extract(tmp_path):

    tdir = Path(tmp_path).resolve()

    out = create_dir(tdir / "out")
    resources_dir = create_dir(tdir / "resources")

    mocked_resources = {
        "brain-region-id": Mock(notation="root"),
        "variant-config-id": None,
        **_mock_atlas_resource(create_dir(resources_dir / "atlas")),
        **_mock_cell_composition_resource(resources_dir),
    }

    with (
        patch("cwl_registry.app.me_type_property.get_forge") as patched_get_kg_forge,
        patch("cwl_registry.Variant.from_resource_id") as patched_variant,
    ):

        patched_get_kg_forge.return_value = Mock(
            retrieve=Mock(side_effect=lambda id, cross_bucket: mocked_resources[id])
        )
        patched_variant.return_value = _mock_variant(resources_dir)

        res = tested._extract(
            brain_region_id="brain-region-id",
            variant_config_id="variant-config-id",
            me_type_densities_id="cell-composition-id",
            atlas_id="atlas-id",
            output_dir=out,
        )

    assert res["region"] == "root"
    assert res["atlas-dir"] == out / "stage/atlas"
    assert set(res["atlas-dir"].iterdir()) == {
        out / "stage/atlas/hierarchy.json",
        out / "stage/atlas/brain_regions.nrrd",
    }
    assert res["me-type-densities-file"] == out / "stage/mtype-densities.json"


def test_me_type_property__transform(tmp_path):

    tdir = Path(tmp_path).resolve()

    variant_dir = create_dir(tdir / "variant")
    variant = _mock_variant(variant_dir)

    stage_dir = create_dir(tdir / "stage")

    me_type_densities_file = stage_dir / "me_type_densities.json"
    _create_me_type_densities(me_type_densities_file)

    out = create_dir(tdir / "out")
    staged_data = {
        "region": "root",
        "atlas-dir": "atlas-dir",
        "variant": variant,
        "me-type-densities-file": me_type_densities_file,
    }
    res = tested._transform(staged_data, output_dir=out)

    # check files have been generated in bioname
    assert set(out.iterdir()) == {
        out / "cell_composition.yaml",
        out / "mtype_taxonomy.tsv",
    }

    # check cell composition
    assert load_yaml(out / "cell_composition.yaml") == {
        "version": "v2",
        "neurons": [
            {
                "density": "L23_BP-DSTUT_densities_v3.nrrd",
                "region": "root",
                "traits": {"mtype": "L23_BP", "etype": "dSTUT"},
            },
            {
                "density": "L23_BP-BIR_densities_v3.nrrd",
                "region": "root",
                "traits": {"mtype": "L23_BP", "etype": "bIR"},
            },
            {
                "density": "L23_DBC-BIR_densities_v3.nrrd",
                "region": "root",
                "traits": {"mtype": "L23_DBC", "etype": "bIR"},
            },
        ],
    }

    # check taxonomy
    taxonomy = pd.read_csv(out / "mtype_taxonomy.tsv", sep=r"\s+", index_col="mtype")

    assert taxonomy.index.tolist() == ["L23_BP", "L23_DBC"]
    assert taxonomy["mClass"].tolist() == ["INT", "INT"]
    assert taxonomy["sClass"].tolist() == ["INH", "INH"]


def test_me_type_property__generate(tmp_path):

    tdir = Path(tmp_path).resolve()
    out = tdir / "out"

    bioname_dir = tdir / "bioname"
    bioname_dir.mkdir()

    atlas_dir = tdir / "atlas"
    atlas_dir.mkdir()

    manifest_file = bioname_dir / "MANIFEST.yml"
    utils.write_json(
        data={"common": {"node_population_name": "root__neurons"}}, filepath=manifest_file
    )

    me_type_densities_file = bioname_dir / "densities.json"
    utils.write_json(data={}, filepath=me_type_densities_file)

    transformed_data = {
        "region": "root",
        "parameters": {
            "soma_placement": "basic",
            "sort_by": ["region", "mtype"],
            "density_factor": 1.0,
            "seed": 0,
        },
        "mtype-taxonomy-file": "None",
        "composition-file": "None",
        "atlas-id": None,
        "atlas-dir": None,
        "me-type-densities-file": me_type_densities_file,
    }

    with (
        patch("cwl_registry.app.me_type_property.subprocess.run"),
        patch("cwl_registry.app.me_type_property.mtype_etype_url_mapping") as mock_me,
        patch("cwl_registry.app.me_type_property._generate_cell_composition_summary"),
        patch("cwl_registry.validation.check_population_name_in_nodes"),
    ):
        mock_me.return_value = ({"id1": "label"}, {"id2": "label"})
        tested._generate(transformed_data, output_dir=out)

    build_dir = out / "build"
    config = load_json(build_dir / "config.json")

    assert config == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(out / "build/nodes.h5"),
                    "populations": {
                        "root__neurons": {"type": "biophysical", "partial": ["cell-properties"]}
                    },
                }
            ]
        },
        "metadata": {"status": "partial"},
    }


def test_me_type_property__register(tmp_path):

    tdir = Path(tmp_path).resolve()

    config_path = tdir / "circuit_config.json"
    config_path.touch()

    summary_file = tdir / "summary_file.json"
    summary_file.touch()

    forge = LocalForge(output_dir=tdir)
    forge.storage["brain-region-id"] = Resource.from_json(
        {
            "id": "brain-region-id",
            "type": "Class",
            "label": "my-region",
            "notation": "myr",
        }
    )
    forge.storage["atlas-release-id"] = Resource.from_json(
        {
            "id": "atlas-release-id",
            "type": "AtlasRelease",
            "label": "my-atlas",
        }
    )
    forge.storage["circuit-id"] = Resource.from_json(
        {
            "id": "circuit-id",
            "type": "DetailedCircuit",
        }
    )
    generated_data = {
        "partial-circuit": config_path,
        "atlas-id": "atlas-release-id",
        "composition-summary-file": summary_file,
    }
    with (patch("cwl_registry.app.me_type_property.get_forge") as mock_forge,):
        mock_forge.return_value = forge
        tested._register("brain-region-id", generated_data, "0")

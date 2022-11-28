import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from cwl_registry import Variant
import pandas as pd
from cwl_registry.app import me_type_property as tested
from cwl_registry.utils import create_dir, write_yaml, write_json, load_yaml, load_json
from kgforge.core import Resource


def _mock_variant(variant_dir):

    config_path = variant_dir / "parameters.yml"
    write_yaml(
        data={
            "step1": {"a": 1},
            "step2": {"b": 2},
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
    write_json(filepath=output_file, data=dataset)


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
    info["me-type-densities-id"] = Mock(
        distribution=Resource(
            type="DataDownload",
            atLocation=Mock(
                location=f"file://{output_file}",
            ),
            encodingFormat="application/json",
            name="me_type_densities.json",
        )
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
        "brain-region-id": Mock(label="root"),
        "variant-config-id": None,
        **_mock_atlas_resource(create_dir(resources_dir / "atlas")),
        **_mock_me_type_densities_resource(resources_dir),
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
            me_type_densities_id="me-type-densities-id",
            atlas_id="atlas-id",
            output_dir=out,
            nexus_base=None,
            nexus_token=None,
            nexus_org=None,
            nexus_project=None,
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

    assert res.keys() == {"bioname-dir", "manifest", "cluster-config"}

    expected_bioname_dir = out / "bioname"

    assert res["bioname-dir"] == expected_bioname_dir

    expected_manifest = {
        "common": {
            "atlas": "atlas-dir",
            "region": "root",
            "node_population_name": "root_neurons",
            "edge_population_name": "root_neurons__chemical_synapse",
            "morph_release": "",
            "synthesis": False,
            "partition": ["left", "right"],
        },
        "step1": {"a": 1},
        "step2": {"b": 2},
    }
    assert res["manifest"] == expected_manifest
    assert res["cluster-config"] == "cluster-config-path"

    # check files have been generated in bioname
    assert set(expected_bioname_dir.iterdir()) == {
        expected_bioname_dir / "MANIFEST.yaml",
        expected_bioname_dir / "cell_composition.yaml",
        expected_bioname_dir / "mtype_taxonomy.tsv",
    }

    # should match the returned manifest
    assert load_yaml(expected_bioname_dir / "MANIFEST.yaml") == expected_manifest

    # check cell composition
    assert load_yaml(expected_bioname_dir / "cell_composition.yaml") == {
        "version": "v2",
        "neurons": [
            {
                "density": "L23_BP-DSTUT_densities_v3.nrrd",
                "region": "root",
                "traits": {"mtype": "L23_BP", "etype": "dSTUT", "layer": 1},
            },
            {
                "density": "L23_BP-BIR_densities_v3.nrrd",
                "region": "root",
                "traits": {"mtype": "L23_BP", "etype": "bIR", "layer": 1},
            },
            {
                "density": "L23_DBC-BIR_densities_v3.nrrd",
                "region": "root",
                "traits": {"mtype": "L23_DBC", "etype": "bIR", "layer": 1},
            },
        ],
    }

    # check taxonomy
    taxonomy = pd.read_csv(
        expected_bioname_dir / "mtype_taxonomy.tsv", sep=r"\s+", index_col="mtype"
    )

    assert taxonomy.index.tolist() == ["L23_BP", "L23_DBC"]
    assert taxonomy["mClass"].tolist() == ["INT", "INT"]
    assert taxonomy["sClass"].tolist() == ["INH", "INH"]


def test_me_type_property__generate(tmp_path):

    tdir = Path(tmp_path).resolve()
    out = tdir / "out"

    transformed_data = {
        "bioname-dir": tdir / "bioname",
        "cluster-config": tdir / "bioname/cluster_config.yaml",
        "manifest": {"common": {"node_population_name": "root__neurons"}},
    }

    with patch("subprocess.run"):
        tested._generate(transformed_data, output_dir=out)

    build_dir = out / "build"
    config = load_json(build_dir / "config.json")

    assert config == {
        "version": 2,
        "manifest": {"$BASE_DIR": "."},
        "networks": {
            "nodes": [
                {
                    "nodes_file": str(out / "build/auxiliary/circuit.somata.h5"),
                    "populations": {
                        "root__neurons": {"type": "biophysical", "partial": ["cell-properties"]}
                    },
                }
            ]
        },
        "metadata": {"status": "partial"},
    }


def test_me_type_property__register():

    generated_data = {
        "partial-circuit": None,
    }

    # not much to do
    with (
        patch("cwl_registry.app.me_type_property.get_forge"),
        patch("cwl_registry.registering.register_partial_circuit"),
    ):
        tested._register("my-region", generated_data, None, None, None, None, "0")

import json
import tempfile
import pandas as pd
import pandas.testing as pdt
from unittest.mock import patch
from blue_cwl import staging as test_module
from pathlib import Path
import pytest
from entity_management import nexus

from unittest.mock import patch, Mock

from blue_cwl.utils import load_json


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def cell_composition_volume_dataset():
    return {
        "hasPart": [
            {
                "@id": "http://uri.interlex.org/base/ilx_0383199",
                "label": "L23_BTC",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://uri.interlex.org/base/ilx_0738199",
                        "label": "bAC",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@type": ["METypeDensity"],
                                "@id": "f3770605-91d8-4f51-befe-d289cd7f0afe",
                                "_rev": 2,
                            }
                        ],
                    }
                ],
            },
            {
                "@id": "http://uri.interlex.org/base/ilx_0383202",
                "label": "L23_LBC",
                "about": ["https://neuroshapes.org/MType"],
                "hasPart": [
                    {
                        "@id": "http://uri.interlex.org/base/ilx_0738199",
                        "label": "bAC",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@type": ["METypeDensity"],
                                "@id": "f3770605-91d8-4f51-befe-d289cd7f0afe",
                                "_rev": 2,
                            }
                        ],
                    },
                    {
                        "@id": "http://uri.interlex.org/base/ilx_0738200",
                        "label": "bSTUT",
                        "about": ["https://neuroshapes.org/EType"],
                        "hasPart": [
                            {
                                "@type": ["METypeDensity"],
                                "@id": "f3770605-91d8-4f51-befe-d289cd7f0afe",
                                "_rev": 2,
                            }
                        ],
                    },
                ],
            },
        ]
    }


@pytest.fixture
def materialized_cell_composition_volume():
    rows = [
        (
            "L23_BTC",
            "bAC",
            "http://uri.interlex.org/base/ilx_0383199",
            "http://uri.interlex.org/base/ilx_0738199",
            "path",
        ),
        (
            "L23_LBC",
            "bAC",
            "http://uri.interlex.org/base/ilx_0383202",
            "http://uri.interlex.org/base/ilx_0738199",
            "path",
        ),
        (
            "L23_LBC",
            "bSTUT",
            "http://uri.interlex.org/base/ilx_0383202",
            "http://uri.interlex.org/base/ilx_0738200",
            "path",
        ),
    ]
    return pd.DataFrame(rows, columns=["mtype", "etype", "mtype_url", "etype_url", "path"])


def test_materialize_cell_composition_volume(
    cell_composition_volume_dataset, materialized_cell_composition_volume, tmp_path
):
    output_file = tmp_path / "materialized_cell_composition_volume.parquet"

    def mock_func(entry_id, *args, **kwargs):
        if entry_id == "f3770605-91d8-4f51-befe-d289cd7f0afe?rev=2":
            return "path"
        raise ValueError(entry_id)

    with patch("blue_cwl.staging.get_distribution_location_path", side_effect=mock_func):
        res1 = test_module.materialize_cell_composition_volume(
            cell_composition_volume_dataset, output_file=output_file
        )
        res2 = pd.read_parquet(output_file)

    pdt.assert_frame_equal(res1, res2)
    pdt.assert_frame_equal(res1, materialized_cell_composition_volume)


@pytest.fixture
def cell_composition_summary():
    return {
        "version": 1,
        "unitCode": {"density": "mm^-3"},
        "hasPart": {
            "http://api.brain-map.org/api/v2/data/Structure/23": {
                "label": "Anterior amygdalar area",
                "notation": "AAA",
                "about": "BrainRegion",
                "hasPart": {
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType": {
                        "label": "GEN_mtype",
                        "about": "MType",
                        "hasPart": {
                            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType": {
                                "label": "GEN_etype",
                                "about": "EType",
                                "composition": {
                                    "neuron": {"density": 11167.27060111258, "count": 5523}
                                },
                            }
                        },
                    },
                    "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {
                        "label": "GIN_mtype",
                        "about": "MType",
                        "hasPart": {
                            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronEType": {
                                "label": "GIN_etype",
                                "about": "EType",
                                "composition": {
                                    "neuron": {"density": 22588.589061799536, "count": 11171}
                                },
                            }
                        },
                    },
                },
            },
            "http://api.brain-map.org/api/v2/data/Structure/935": {
                "label": "Anterior cingulate area, dorsal part, layer 1",
                "notation": "ACAd1",
                "about": "BrainRegion",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0383192": {
                        "label": "L1_DAC",
                        "about": "MType",
                        "hasPart": {
                            "http://uri.interlex.org/base/ilx_0738203": {
                                "label": "bNAC",
                                "about": "EType",
                                "composition": {
                                    "neuron": {"density": 728.0265252380789, "count": 434}
                                },
                            }
                        },
                    }
                },
            },
        },
    }


@pytest.fixture
def materialized_cell_composition_summary():
    rows = [
        (
            "AAA",
            "http://api.brain-map.org/api/v2/data/Structure/23",
            "Anterior amygdalar area",
            "GEN_mtype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType",
            "GEN_etype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType",
            11167.27060111258,
        ),
        (
            "AAA",
            "http://api.brain-map.org/api/v2/data/Structure/23",
            "Anterior amygdalar area",
            "GIN_mtype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType",
            "GIN_etype",
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronEType",
            22588.589061799536,
        ),
        (
            "ACAd1",
            "http://api.brain-map.org/api/v2/data/Structure/935",
            "Anterior cingulate area, dorsal part, layer 1",
            "L1_DAC",
            "http://uri.interlex.org/base/ilx_0383192",
            "bNAC",
            "http://uri.interlex.org/base/ilx_0738203",
            728.0265252380789,
        ),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "region",
            "region_url",
            "region_label",
            "mtype",
            "mtype_url",
            "etype",
            "etype_url",
            "density",
        ],
    )


def test_materialize_cell_composition_summary(
    cell_composition_summary, materialized_cell_composition_summary, tmp_path
):
    output_file = tmp_path / "materialized_cell_composition_summary.parquet"
    res1 = test_module.materialize_cell_composition_summary(
        cell_composition_summary, output_file=output_file
    )
    res2 = pd.read_parquet(output_file)

    pdt.assert_frame_equal(res1, res2)
    pdt.assert_frame_equal(res1, materialized_cell_composition_summary)


def _materialize_connectome_config(config):
    with patch("blue_cwl.staging._config_to_path") as mock:
        mock.return_value = "foo"

        with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
            out_file = Path(tfile.name)

            res = test_module.materialize_macro_connectome_config(config, output_file=out_file)
            return res, json.loads(Path(out_file).read_bytes())


def test_materialize_macro_connectome_config():
    config = {
        "initial": {
            "connection_strength": {
                "id": "cs-id",
                "type": ["Entity", "Dataset", "BrainConnectomeStrength"],
            }
        },
        "overrides": {
            "connection_strength": {
                "id": "cs-overrides-id",
                "type": ["Entity", "Dataset", "BrainConnectomeStrengthOverrides"],
            }
        },
    }

    res1, res2 = _materialize_connectome_config(config)

    assert (
        res1
        == res2
        == {
            "initial": {"connection_strength": "foo"},
            "overrides": {"connection_strength": "foo"},
        }
    )


def test_materialize_macro_connectome_config_old():
    config = {
        "bases": {
            "connection_strength": {
                "id": "cs-id",
                "type": ["Entity", "Dataset", "BrainConnectomeStrength"],
            }
        },
        "overrides": {
            "connection_strength": {
                "id": "cs-overrides-id",
                "type": ["Entity", "Dataset", "BrainConnectomeStrengthOverrides"],
            }
        },
    }

    res1, res2 = _materialize_connectome_config(config)

    assert (
        res1
        == res2
        == {
            "initial": {"connection_strength": "foo"},
            "overrides": {"connection_strength": "foo"},
        }
    )


def test_materialize_macro_connectome_config__empty_overrides():
    config = {
        "bases": {
            "connection_strength": {
                "id": "cs-id",
                "type": ["Entity", "Dataset", "BrainConnectomeStrength"],
            }
        },
        "overrides": {},
    }

    res1, res2 = _materialize_connectome_config(config)

    assert (
        res1
        == res2
        == {
            "initial": {"connection_strength": "foo"},
            "overrides": {},
        }
    )


def _materialize_micro_config(config):
    def mock_config_to_path(config, *args, **kwargs):
        assert config
        return "foo"

    with patch("blue_cwl.staging._config_to_path", side_effect=mock_config_to_path):
        with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
            out_file = Path(tfile.name)

            res = test_module.materialize_micro_connectome_config(obj=config, output_file=out_file)
            return res, json.loads(Path(out_file).read_bytes())


def test_materialize_micro_connectome_config__no_overrides():
    config = {
        "variants": {
            "placeholder__erdos_renyi": {},
            "placeholder__distance_dependent": {},
        },
        "initial": {
            "variants": {
                "id": "v-id",
                "rev": 5,
                "type": ["Entity", "Dataset", "MicroConnectomeVariantSelection"],
            },
            "placeholder__erdos_renyi": {
                "id": "er-id",
                "rev": 2,
                "type": ["Entity", "Dataset", "MicroConnectomeData"],
            },
            "placeholder__distance_dependent": {
                "id": "dd-id",
                "rev": 2,
                "type": ["Entity", "Dataset", "MicroConnectomeData"],
            },
        },
        "overrides": {},
    }

    res1, res2 = _materialize_micro_config(config)

    assert (
        res1
        == res2
        == {
            "variants": {"placeholder__erdos_renyi": {}, "placeholder__distance_dependent": {}},
            "initial": {
                "variants": "foo",
                "placeholder__erdos_renyi": "foo",
                "placeholder__distance_dependent": "foo",
            },
            "overrides": {},
        }
    )


def test_materialize_micro_connectome_config__no_overrides2():
    config = {
        "variants": {
            "placeholder__erdos_renyi": {},
            "placeholder__distance_dependent": {},
        },
        "initial": {
            "placeholder__erdos_renyi": {
                "id": "https://bbp.epfl.ch/neurosciencegraph/data/microconnectomedata/009413eb-e51b-40bc-9199-8b98bfc53f87",
                "rev": 7,
                "type": ["Entity", "Dataset", "MicroConnectomeData"],
            },
            "variants": {
                "id": "https://bbp.epfl.ch/neurosciencegraph/data/a46a442c-5baa-4a5c-9907-bfb359dd9e5d",
                "rev": 9,
                "type": ["Entity", "Dataset", "MicroConnectomeVariantSelection"],
            },
            "placeholder__distance_dependent": {
                "id": "https://bbp.epfl.ch/neurosciencegraph/data/microconnectomedata/c7e1d215-2dad-4216-8565-6b1e4c161f46",
                "rev": 7,
                "type": ["Entity", "Dataset", "MicroConnectomeData"],
            },
        },
        "overrides": {
            "placeholder__erdos_renyi": {},
            "variants": {},
            "placeholder__distance_dependent": {},
        },
    }

    res1, res2 = _materialize_micro_config(config)

    assert (
        res1
        == res2
        == {
            "variants": {"placeholder__erdos_renyi": {}, "placeholder__distance_dependent": {}},
            "initial": {
                "variants": "foo",
                "placeholder__erdos_renyi": "foo",
                "placeholder__distance_dependent": "foo",
            },
            "overrides": {},
        }
    )


def test_materialize_micro_connectome_config__no_variant_overrides():
    config = {
        "variants": {
            "placeholder__erdos_renyi": {},
            "placeholder__distance_dependent": {},
        },
        "initial": {
            "variants": {
                "id": "v-id",
                "rev": 5,
                "type": ["Entity", "Dataset", "MicroConnectomeVariantSelection"],
            },
            "configuration": {
                "placeholder__erdos_renyi": {
                    "id": "er-id",
                    "rev": 2,
                    "type": ["Entity", "Dataset", "MicroConnectomeData"],
                },
                "placeholder__distance_dependent": {
                    "id": "dd-id",
                    "rev": 2,
                    "type": ["Entity", "Dataset", "MicroConnectomeData"],
                },
            },
        },
        "overrides": {
            "configuration": {
                "placeholder__erdos_renyi": {
                    "id": "er-overrides-id",
                    "type": ["Entity", "Dataset", "MicroConnectomeDataOverrides"],
                    "rev": 1,
                },
                "placeholder__distance_dependent": {
                    "id": "dd-overrides-id",
                    "type": ["Entity", "Dataset", "MicroConnectomeDataOverrides"],
                    "rev": 1,
                },
            }
        },
    }

    res1, res2 = _materialize_micro_config(config)

    assert (
        res1
        == res2
        == {
            "variants": {"placeholder__erdos_renyi": {}, "placeholder__distance_dependent": {}},
            "initial": {
                "variants": "foo",
                "placeholder__erdos_renyi": "foo",
                "placeholder__distance_dependent": "foo",
            },
            "overrides": {
                "placeholder__erdos_renyi": "foo",
                "placeholder__distance_dependent": "foo",
            },
        }
    )


def test_materialize_micro_connectome_config__no_er_overrides():
    config = {
        "variants": {
            "placeholder__erdos_renyi": {},
            "placeholder__distance_dependent": {},
        },
        "initial": {
            "variants": {
                "id": "v-id",
                "rev": 5,
                "type": ["Entity", "Dataset", "MicroConnectomeVariantSelection"],
            },
            "configuration": {
                "placeholder__erdos_renyi": {
                    "id": "er-id",
                    "rev": 2,
                    "type": ["Entity", "Dataset", "MicroConnectomeData"],
                },
                "placeholder__distance_dependent": {
                    "id": "dd-id",
                    "rev": 2,
                    "type": ["Entity", "Dataset", "MicroConnectomeData"],
                },
            },
        },
        "overrides": {
            "variants": {
                "id": "v-overrides-id",
                "type": ["Entity", "Dataset", "MicroConnectomeVariantSelectionOverrides"],
                "rev": 1,
            },
            "configuration": {
                "placeholder__distance_dependent": {
                    "id": "dd-overrides-id",
                    "type": ["Entity", "Dataset", "MicroConnectomeDataOverrides"],
                    "rev": 1,
                }
            },
        },
    }

    res1, res2 = _materialize_micro_config(config)

    assert (
        res1
        == res2
        == {
            "variants": {"placeholder__erdos_renyi": {}, "placeholder__distance_dependent": {}},
            "initial": {
                "variants": "foo",
                "placeholder__erdos_renyi": "foo",
                "placeholder__distance_dependent": "foo",
            },
            "overrides": {"variants": "foo", "placeholder__distance_dependent": "foo"},
        }
    )


@pytest.fixture
def json_synapse_config():
    return load_json(DATA_DIR / "synapse_config.json")


def test_materialize_synapse_config(json_synapse_config):
    with patch(
        "blue_cwl.staging.stage_distribution_file",
        side_effect=lambda *args, **kwargs: args[0],
    ):
        res = test_module.materialize_synapse_config(obj=json_synapse_config, output_dir=None)

        assert res == {
            "defaults": {
                "synapse_properties": "https://bbp.epfl.ch/neurosciencegraph/data/synapticassignment/d57536aa-d576-4b3b-a89b-b7888f24eb21?rev=9",
                "synapses_classification": "https://bbp.epfl.ch/neurosciencegraph/data/synapticparameters/cf25c2bf-e6e4-4367-acd8-94004bfcfe49?rev=6",
            },
            "configuration": {
                "synapse_properties": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/f2bce285-380d-40da-95db-c8af2013f21e?rev=1",
                "synapses_classification": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/d133e408-bd00-41ca-9334-e5fab779ad99?rev=1",
            },
        }


@pytest.fixture
def json_ph_catalog():
    return load_json(DATA_DIR / "placement_hints_catalog.json")


def test_materialize_placement_hints_catalog(json_ph_catalog):
    res = test_module.materialize_ph_catalog(json_ph_catalog)

    expected = {
        "placement_hints": [
            {
                "path": "/[PH]layer_1.nrrd",
                "regions": {
                    "Isocortex": {"hasLeafRegionPart": ["SSp-ll1", "AUDd1"], "layer": "L1"}
                },
            },
            {
                "path": "/[PH]layer_2.nrrd",
                "regions": {
                    "Isocortex": {
                        "hasLeafRegionPart": ["PL2", "ILA2", "ORBm2", "RSPv2"],
                        "layer": "L2",
                    }
                },
            },
            {
                "path": "/[PH]layer_3.nrrd",
                "regions": {"Isocortex": {"hasLeafRegionPart": ["FRP3", "MOp3"], "layer": "L3"}},
            },
            {
                "path": "/[PH]layer_4.nrrd",
                "regions": {
                    "Isocortex": {"hasLeafRegionPart": ["AUDp4", "SSp-ul4"], "layer": "L4"}
                },
            },
            {
                "path": "/[PH]layer_5.nrrd",
                "regions": {
                    "Isocortex": {"hasLeafRegionPart": ["VISpor5", "ORBm5"], "layer": "L5"}
                },
            },
            {
                "path": "/[PH]layer_6.nrrd",
                "regions": {"Isocortex": {"hasLeafRegionPart": ["ACA6b", "AUDp6a"], "layer": "L6"}},
            },
        ],
        "voxel_distance_to_region_bottom": {"path": "/[PH]y.nrrd"},
    }

    assert res == expected


def test_materialize_placement_hints_catalog__output_dir(json_ph_catalog):
    with patch("blue_cwl.staging.stage_file"):
        res = test_module.materialize_ph_catalog(obj=json_ph_catalog, output_dir="/my-dir")

        expected = {
            "placement_hints": [
                {
                    "path": "/my-dir/[PH]1.nrrd",
                    "regions": {
                        "Isocortex": {"hasLeafRegionPart": ["SSp-ll1", "AUDd1"], "layer": "L1"}
                    },
                },
                {
                    "path": "/my-dir/[PH]2.nrrd",
                    "regions": {
                        "Isocortex": {
                            "hasLeafRegionPart": ["PL2", "ILA2", "ORBm2", "RSPv2"],
                            "layer": "L2",
                        }
                    },
                },
                {
                    "path": "/my-dir/[PH]3.nrrd",
                    "regions": {
                        "Isocortex": {"hasLeafRegionPart": ["FRP3", "MOp3"], "layer": "L3"}
                    },
                },
                {
                    "path": "/my-dir/[PH]4.nrrd",
                    "regions": {
                        "Isocortex": {"hasLeafRegionPart": ["AUDp4", "SSp-ul4"], "layer": "L4"}
                    },
                },
                {
                    "path": "/my-dir/[PH]5.nrrd",
                    "regions": {
                        "Isocortex": {"hasLeafRegionPart": ["VISpor5", "ORBm5"], "layer": "L5"}
                    },
                },
                {
                    "path": "/my-dir/[PH]6.nrrd",
                    "regions": {
                        "Isocortex": {"hasLeafRegionPart": ["ACA6b", "AUDp6a"], "layer": "L6"}
                    },
                },
            ],
            "voxel_distance_to_region_bottom": {"path": "/my-dir/[PH]y.nrrd"},
        }

        assert res == expected

import json
import tempfile
import pandas
import pandas.testing
from unittest.mock import patch
from cwl_registry import staging as test_module
from pathlib import Path
import pytest

from unittest.mock import patch, Mock

from cwl_registry.utils import load_json

from tests.unit.mocking import LocalForge

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def cell_composition_volume_dataset(tmp_path):
    return {
        "hasPart": [
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
                ],
            },
        ]
    }


def test_materialize_grouped_dataset__cell_composition_volume(
    tmp_path, cell_composition_volume_dataset
):
    mock = Mock(
        id="f3770605-91d8-4f51-befe-d289cd7f0afe?rev=2",
        atLocation=Mock(location=Mock(contenUrl=None)),
    )

    forge = LocalForge()
    forge.register(resource=mock)
    with patch("entity_management.nexus.get_file_location") as patched:
        patched.return_value = "file:///my%5Bpath%5D"
        result_wout_groups = test_module.apply_to_grouped_dataset(
            forge, cell_composition_volume_dataset, group_names=None
        )
        result_with_groups = test_module.apply_to_grouped_dataset(
            forge, cell_composition_volume_dataset, group_names=("mtypes", "etypes")
        )

    assert result_wout_groups == {
        "hasPart": {
            "http://uri.interlex.org/base/ilx_0383202": {
                "label": "L23_LBC",
                "hasPart": {
                    "http://uri.interlex.org/base/ilx_0738199": {
                        "label": "bAC",
                        "path": "/my[path]",
                    },
                    "http://uri.interlex.org/base/ilx_0738200": {
                        "label": "bAC",
                        "path": "/my[path]",
                    },
                },
            }
        }
    }
    assert result_with_groups == {
        "mtypes": {
            "http://uri.interlex.org/base/ilx_0383202": {
                "label": "L23_LBC",
                "etypes": {
                    "http://uri.interlex.org/base/ilx_0738199": {
                        "label": "bAC",
                        "path": "/my[path]",
                    },
                    "http://uri.interlex.org/base/ilx_0738200": {
                        "label": "bAC",
                        "path": "/my[path]",
                    },
                },
            }
        }
    }


def _materialize_connectome_config(config):
    with patch("cwl_registry.staging.read_json_file_from_resource_id") as mock1, patch(
        "cwl_registry.staging._config_to_path"
    ) as mock2:
        mock1.return_value = config
        mock2.return_value = "foo"

        with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
            out_file = Path(tfile.name)

            res = test_module.materialize_macro_connectome_config(None, "bar", output_file=out_file)
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
    def mock_config_to_path(forge, variant_data):
        assert variant_data
        return "foo"

    with (
        patch("cwl_registry.staging.read_json_file_from_resource_id") as mock1,
        patch("cwl_registry.staging._config_to_path", side_effect=mock_config_to_path),
    ):
        mock1.return_value = config

        with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
            out_file = Path(tfile.name)

            res = test_module.materialize_micro_connectome_config(None, "bar", output_file=out_file)
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
        "cwl_registry.staging.read_json_file_from_resource_id", return_value=json_synapse_config
    ), patch(
        "cwl_registry.staging.stage_resource_distribution_file",
        side_effect=lambda *args, **kwargs: kwargs["resource_id"],
    ):
        res = test_module.materialize_synapse_config(None, None, None)

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
    with patch(
        "cwl_registry.staging.read_json_file_from_resource_id", return_value=json_ph_catalog
    ):
        res = test_module.materialize_ph_catalog(None, None)

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
    with (
        patch("cwl_registry.staging.read_json_file_from_resource_id", return_value=json_ph_catalog),
        patch("cwl_registry.staging.stage_file"),
    ):
        res = test_module.materialize_ph_catalog(None, None, output_dir="/my-dir")

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

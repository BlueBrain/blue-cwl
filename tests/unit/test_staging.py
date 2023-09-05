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
    with patch("cwl_registry.staging.get_file_location") as patched:
        patched.return_value = "my-path"
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
                    "http://uri.interlex.org/base/ilx_0738199": {"label": "bAC", "path": "my-path"},
                    "http://uri.interlex.org/base/ilx_0738200": {"label": "bAC", "path": "my-path"},
                },
            }
        }
    }
    assert result_with_groups == {
        "mtypes": {
            "http://uri.interlex.org/base/ilx_0383202": {
                "label": "L23_LBC",
                "etypes": {
                    "http://uri.interlex.org/base/ilx_0738199": {"label": "bAC", "path": "my-path"},
                    "http://uri.interlex.org/base/ilx_0738200": {"label": "bAC", "path": "my-path"},
                },
            }
        }
    }


from kgforge.core import Resource


def test_materialize_connectome_dataset(tmp_path):
    outfile = Path(tmp_path, "materialized_connectome_dataset.json")

    dataset = json.loads(Path(DATA_DIR, "connectome_dataset.json").read_bytes())

    mock_forge = LocalForge()
    mock_forge.register(
        Resource(
            id="http://api.brain-map.org/api/v2/data/Structure/23",
            notation="AAA",
        ),
    )
    mock_forge.register(
        Resource(
            id="http://api.brain-map.org/api/v2/data/Structure/935",
            notation="ACAd1",
        ),
    )
    mock_forge.register(
        Resource(
            id="http://uri.interlex.org/base/ilx_0383231?rev=34",
            label="L6_DBC",
        ),
    )
    data = test_module.materialize_connectome_dataset(mock_forge, dataset, output_file=outfile)

    data_from_file = pandas.read_json(outfile, orient="records")
    pandas.testing.assert_frame_equal(data, data_from_file)

    expected = pandas.DataFrame(
        {
            "hi": ["left", "right"],
            "hj": ["left", "left"],
            "ri": ["AAA", "AAA"],
            "rj": ["ACAd1", "ACAd1"],
            "mi": ["L6_DBC", "L6_DBC"],
            "mj": ["L6_DBC", "L6_DBC"],
            "scale": [1, 1],
            "exponent": [1, 1],
            "mean_synapses_per_connection": [100, 100],
            "sdev_synapses_per_connection": [1, 1],
            "mean_conductance_velocity": [0.3, 0.3],
            "sdev_conductance_velocity": [0.01, 0.01],
            "seed": [0, 0],
        }
    )
    pandas.testing.assert_frame_equal(expected, data)


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
    with patch("cwl_registry.staging.read_json_file_from_resource_id") as mock1, patch(
        "cwl_registry.staging._config_to_path"
    ) as mock2:
        mock1.return_value = config
        mock2.return_value = "foo"

        with tempfile.NamedTemporaryFile(suffix=".json") as tfile:
            out_file = Path(tfile.name)

            res = test_module.materialize_micro_connectome_config(None, "bar", output_file=out_file)
            return res, json.loads(Path(out_file).read_bytes())


def test_materialize_micro_connectome_config():
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
            "overrides": {
                "variants": "foo",
                "placeholder__erdos_renyi": "foo",
                "placeholder__distance_dependent": "foo",
            },
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

import json
import pandas
import pandas.testing
from cwl_registry import staging as test_module
from pathlib import Path
import pytest

from unittest.mock import patch, Mock

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

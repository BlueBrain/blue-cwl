from cwl_registry import staging as test_module

import pytest

from unittest.mock import patch, Mock

from tests.unit.mocking import LocalForge


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

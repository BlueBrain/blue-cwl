import re
import pytest
from cwl_registry.exceptions import CWLRegistryError

from cwl_registry.wrappers import cell_composition_manipulation as test_module


def test_check_recipe_compatibility_with_density_distribution__correct():
    densities = {
        "mtypes": {
            "mtype1_id": {
                "label": "mtype1",
                "etypes": {
                    "etype1_id": {"label": "etype1"},
                    "etype2_id": {"label": "etype2"},
                },
            },
            "mtype2_id": {
                "label": "mtype2",
                "etypes": {
                    "etype1_id": {"label": "etype1"},
                    "etype2_id": {"label": "etype2"},
                    "etype3_id": {"label": "etype3"},
                },
            },
        }
    }

    recipe = {
        "overrides": {
            "region1": {
                "hasPart": {
                    "mtype1_id": {
                        "label": "mtype1",
                        "hasPart": {
                            "etype1_id": {"label": "etype1"},
                            "etype2_id": {"label": "etype2"},
                        },
                    },
                    "mtype2_id": {
                        "label": "mtype2",
                        "hasPart": {
                            "etype2_id": {"label": "etype2"},
                            "etype3_id": {"label": "etype3"},
                        },
                    },
                }
            }
        }
    }
    test_module._check_recipe_compatibility_with_density_distribution(densities, recipe)


def test_check_recipe_compatibility_with_density_distribution__missing():
    densities = {
        "mtypes": {
            "mtype1_id": {
                "label": "mtype1",
                "etypes": {
                    "etype2_id": {"label": "etype2"},
                },
            },
            "mtype2_id": {
                "label": "mtype2",
                "etypes": {
                    "etype1_id": {"label": "etype1"},
                    "etype2_id": {"label": "etype2"},
                    "etype3_id": {"label": "etype3"},
                },
            },
        }
    }

    recipe = {
        "overrides": {
            "region1": {
                "hasPart": {
                    "mtype1_id": {
                        "label": "mtype1",
                        "hasPart": {
                            "etype1_id": {"label": "etype1"},
                            "etype2_id": {"label": "etype2"},
                        },
                    },
                    "mtype2_id": {
                        "label": "mtype2",
                        "hasPart": {
                            "etype2_id": {"label": "etype2"},
                            "etype3_id": {"label": "etype3"},
                        },
                    },
                }
            }
        }
    }
    contains_str = re.escape("Missing entries: [('mtype1_id=mtype1', 'etype1_id=etype1')]")
    with pytest.raises(CWLRegistryError, match=contains_str):
        test_module._check_recipe_compatibility_with_density_distribution(densities, recipe)

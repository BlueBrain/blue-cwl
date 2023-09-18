import re
from unittest.mock import Mock, patch

import pytest
from cwl_registry.exceptions import CWLRegistryError, SchemaValidationError, CWLWorkflowError

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


def test_validate_cell_composition_schemas():
    cell_composition = Mock()
    cell_composition.cellCompositionVolume.id = "foo"
    cell_composition.cellCompositionSummary.id = "bar"

    with (
        patch("cwl_registry.wrappers.cell_composition_manipulation.validate_schema"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.read_json_file_from_resource"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.get_resource"),
    ):
        test_module._validate_cell_composition_schemas(cell_composition, None)


def test_validate_cell_composition_volume_schema():
    with (
        patch("cwl_registry.wrappers.cell_composition_manipulation.validate_schema"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.get_resource"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.read_json_file_from_resource"),
    ):
        test_module._validate_cell_composition_volume_schema(None, None)


def test_validate_cell_composition_volume_schema__raises():
    volume_id = "volume-id"

    expected_error = (
        "Schema validation failed for CellComposition's volume distribution.\n"
        "CellCompositionVolume failing the validation: volume-id"
    )
    with (
        patch(
            "cwl_registry.wrappers.cell_composition_manipulation.validate_schema",
            side_effect=SchemaValidationError("foo"),
        ),
        patch("cwl_registry.wrappers.cell_composition_manipulation.get_resource"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.read_json_file_from_resource"),
    ):
        with pytest.raises(CWLWorkflowError, match=re.escape(expected_error)):
            test_module._validate_cell_composition_volume_schema(volume_id, None)


def test_validate_cell_composition_summary_schema():
    with (
        patch("cwl_registry.wrappers.cell_composition_manipulation.validate_schema"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.get_resource"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.read_json_file_from_resource"),
    ):
        test_module._validate_cell_composition_summary_schema(None, None)


def test_validate_cell_composition_summary_schema__raises():
    summary_id = "summary-id"

    expected_error = (
        "Schema validation failed for CellComposition's summary.\n"
        "CellCompositionSummary failing the validation: summary-id"
    )
    with (
        patch(
            "cwl_registry.wrappers.cell_composition_manipulation.validate_schema",
            side_effect=SchemaValidationError("bar"),
        ),
        patch("cwl_registry.wrappers.cell_composition_manipulation.get_resource"),
        patch("cwl_registry.wrappers.cell_composition_manipulation.read_json_file_from_resource"),
    ):
        with pytest.raises(CWLWorkflowError, match=re.escape(expected_error)):
            test_module._validate_cell_composition_summary_schema(summary_id, None)

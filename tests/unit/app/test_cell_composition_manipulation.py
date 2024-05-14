import re
from unittest.mock import Mock, patch

import pytest
import pandas as pd
from blue_cwl.exceptions import CWLRegistryError, SchemaValidationError, CWLWorkflowError

from blue_cwl.wrappers import cell_composition_manipulation as test_module
from blue_cwl.density_manipulation import read_density_manipulation_recipe


def test_check_recipe_compatibility_with_density_distribution__correct():
    densities = pd.DataFrame(
        [
            ("mtype1", "etype1"),
            ("mtype1", "etype2"),
            ("mtype2", "etype1"),
            ("mtype2", "etype2"),
            ("mtype2", "etype3"),
        ],
        columns=["mtype", "etype"],
    )

    recipe = pd.DataFrame(
        [
            ["region1", "mtype1", "etype1"],
            ["region1", "mtype1", "etype2"],
            ["region1", "mtype2", "etype2"],
            ["region1", "mtype2", "etype3"],
        ],
        columns=["region", "mtype", "etype"],
    )

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
    densities = pd.DataFrame(
        [
            ("mtype1", "mtype1_id", "etype2", "etype2_id"),
            ("mtype2", "mtype2_id", "etype1", "etype1_id"),
            ("mtype2", "mtype2_id", "etype2", "etype2_id"),
            ("mtype2", "mtype2_id", "etype3", "etype2_id"),
        ],
        columns=["mtype", "mtype_url", "etype", "etype_url"],
    )

    recipe = pd.DataFrame(
        [
            ["region1", "mtype1", "mtype1_id", "etype1", "etype1_id"],
            ["region1", "mtype1", "mtype1_id", "etype2", "etype2_id"],
            ["region1", "mtype2", "mtype2_id", "etype2", "etype2_id"],
            ["region1", "mtype2", "mtype2_id", "etype3", "etype3_id"],
        ],
        columns=["region", "mtype", "mtype_url", "etype", "etype_url"],
    )

    contains_str = re.escape("('mtype1_id=mtype1', 'etype1_id=etype1')")
    with pytest.raises(CWLRegistryError, match=contains_str):
        test_module._check_recipe_compatibility_with_density_distribution(densities, recipe)


def test_validate_cell_composition_schemas():
    cell_composition = Mock()
    cell_composition.cellCompositionVolume.get_id.return_value = "foo"
    cell_composition.cellCompositionSummary.get_id.return_value = "bar"

    with (
        patch("blue_cwl.wrappers.cell_composition_manipulation.validate_schema"),
        patch("blue_cwl.wrappers.cell_composition_manipulation.get_distribution_as_dict"),
    ):
        test_module._validate_cell_composition_schemas(cell_composition)


def test_validate_cell_composition_volume_schema():
    with (
        patch("blue_cwl.wrappers.cell_composition_manipulation.validate_schema"),
        patch("blue_cwl.wrappers.cell_composition_manipulation.get_distribution_as_dict"),
    ):
        test_module._validate_cell_composition_volume_schema(None)


def test_validate_cell_composition_volume_schema__raises():
    volume_id = "volume-id"

    expected_error = (
        "Schema validation failed for CellComposition's volume distribution.\n"
        "CellCompositionVolume failing the validation: volume-id"
    )
    with (
        patch(
            "blue_cwl.wrappers.cell_composition_manipulation.validate_schema",
            side_effect=SchemaValidationError("foo"),
        ),
        patch("blue_cwl.wrappers.cell_composition_manipulation.get_distribution_as_dict"),
    ):
        with pytest.raises(CWLWorkflowError, match=re.escape(expected_error)):
            test_module._validate_cell_composition_volume_schema(volume_id)


def test_validate_cell_composition_summary_schema():
    with (
        patch("blue_cwl.wrappers.cell_composition_manipulation.validate_schema"),
        patch("blue_cwl.wrappers.cell_composition_manipulation.get_distribution_as_dict"),
    ):
        test_module._validate_cell_composition_summary_schema(None)


def test_validate_cell_composition_summary_schema__raises():
    summary_id = "summary-id"

    expected_error = (
        "Schema validation failed for CellComposition's summary.\n"
        "CellCompositionSummary failing the validation: summary-id"
    )
    with (
        patch(
            "blue_cwl.wrappers.cell_composition_manipulation.validate_schema",
            side_effect=SchemaValidationError("bar"),
        ),
        patch("blue_cwl.wrappers.cell_composition_manipulation.get_distribution_as_dict"),
    ):
        with pytest.raises(CWLWorkflowError, match=re.escape(expected_error)):
            test_module._validate_cell_composition_summary_schema(summary_id)

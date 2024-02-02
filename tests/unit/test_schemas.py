import pytest
from pathlib import Path
from cwl_registry import validation as test_module
from cwl_registry.utils import load_json

DATA_DIR = Path(__file__).parent / "data/schemas"


@pytest.mark.parametrize(
    "name",
    [
        "cell_composition_volume_distribution",
        "cell_composition_summary_distribution",
        "placeholder_morphology_config_distribution_v2",
        "canonical_morphology_model_config_distribution_v2",
        "morphology_assignment_config_distribution",
        "me_model_config_distribution",
        "placeholder_emodel_config_distribution",
        "brain_region_selector_config_distribution",
    ],
)
def test_schema(name):
    schema = f"{name}.yml"
    dataset = load_json(DATA_DIR / f"{name}.json")
    test_module.validate_schema(data=dataset, schema_name=schema)

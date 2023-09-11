from pathlib import Path

import pytest

from entity_management import nexus

from cwl_registry.utils import load_json
from cwl_registry.mmodel import schemas, entity


DATA_DIR = Path(__file__).parent / "data"


def _init_entity_from_data(monkeypatch, entity_class, data):
    """Create an entity using a local metadata file instead of talking to nexus."""
    monkeypatch.setattr(nexus, "load_by_url", lambda *args, **kwargs: data)
    return entity_class.from_url(None)


@pytest.fixture
def morphology_assignment_config_metadata():
    """JSONLD metadata for MorphologyAssignmentConfig."""
    return load_json(DATA_DIR / "morphology_assignment_config_metadata.json")


@pytest.fixture
def morphology_assignment_config_distribution_location():
    path = DATA_DIR / "morphology_assignment_config_distribution.json"
    return f"file://{path}"


@pytest.fixture
def morphology_assignment_config_distribution(morphology_assignment_config_distribution_location):
    """JSON data for MorphologyAssignmentConfig distribution."""
    return load_json(morphology_assignment_config[7:])


@pytest.fixture
def morphology_assignment_config(monkeypatch, morphology_assignment_config_metadata):
    """MorphologyAssignmentConfig entity."""
    return _init_entity_from_data(
        monkeypatch, entity.MorphologyAssignmentConfig, morphology_assignment_config_metadata
    )


@pytest.fixture
def canonical_morphology_model_config_metadata():
    """JSONLD metdata for CanonicalMorphologyModelConfig."""
    return load_json(DATA_DIR / "canonical_morphology_model_config_metadata.json")


@pytest.fixture
def canonical_morphology_model_config(monkeypatch, canonical_morphology_model_config_metadata):
    """CanonicalMorphologyModelConfig local entity."""
    return _init_entity_from_data(
        monkeypatch,
        entity.CanonicalMorphologyModelConfig,
        canonical_morphology_model_config_metadata,
    )


@pytest.fixture
def canonical_morphology_model_config_distribution_location():
    """Location to CanonicalMorphologyModelConfig distribution json file."""
    path = DATA_DIR / "canonical_models.json"
    return f"file://{path}"


@pytest.fixture
def canonical_morphology_model_config_distribution(
    canonical_morphology_model_config_distribution_location,
):
    """CanonicalMorphologyConfig distribution local data."""
    return load_json(canonical_morphology_model_config_distribution_location[7:])


@pytest.fixture
def canonical_distribution_config(canonical_morphology_model_config_distribution):
    """CanonicalMorphologConfig distribution schema instance."""
    return schemas.CanonicalDistributionConfig.parse_obj(
        canonical_morphology_model_config_distribution
    )


@pytest.fixture
def placeholder_morphology_config_metadata():
    """PlaceholderMorphologyConfig entity local metadata."""
    return load_json(DATA_DIR / "placeholder_morphology_config_metadata.json")


@pytest.fixture
def placeholder_morphology_config(monkeypatch, placeholder_morphology_config_metadata):
    """PlaceholderMorphologyConfig local entity."""
    return _init_entity_from_data(
        monkeypatch, entity.PlaceholderMorphologyConfig, placeholder_morphology_config_metadata
    )


@pytest.fixture
def placeholder_morphology_config_distribution_location():
    """Placeholders json configuration json location."""
    path = DATA_DIR / "placeholders.json"
    return f"file://{path}"


@pytest.fixture
def placeholder_morphology_config_distribution(placeholder_morphology_config_distribution_location):
    """PlaceholderMorphologyConfig distribution data class."""
    return load_json(placeholder_morphology_config_distribution_location[7:])


@pytest.fixture
def placeholder_distribution_config(placeholder_morphology_config_distribution):
    """PlaceholderMorphologyConfig distribution data."""
    return schemas.PlaceholderDistributionConfig.parse_obj(
        placeholder_morphology_config_distribution
    )


@pytest.fixture
def parameters():
    """Single cell topological synthesis input parameters."""
    return load_json(DATA_DIR / "parameters.json")


@pytest.fixture
def distributions():
    """Single cell topological synthesis input distributions."""
    return load_json(DATA_DIR / "distributions.json")

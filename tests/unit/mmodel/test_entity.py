import json
import pytest
from pathlib import Path
from unittest.mock import patch, Mock

from cwl_registry.mmodel import entity as test_module
from cwl_registry.mmodel import schemas

from entity_management import nexus

URL_PREFIX = "https://bbp.epfl.ch"


def test_distribution_to_model():
    """Test creation of schema from entity distribution's path."""
    distribution = Mock()
    distribution.get_location = lambda: "file://my-path"

    schema_class = Mock()
    schema_class.parse_file = lambda path: path

    res = test_module._distribution_to_model(distribution, schema_class)

    assert res == "my-path"


def _check_to_model_called(monkeypatch, location, entity, schema_name):
    monkeypatch.setattr(nexus, "get_file_location", lambda *args, **kwargs: location)

    with patch(f"cwl_registry.mmodel.schemas.{schema_name}.parse_file") as patched:
        model = entity.to_model()
        patched.assert_called_once_with(location[7:])


def test_morphology_assignment_config(morphology_assignment_config):
    """Test that the initialized entity has a valid id."""
    assert morphology_assignment_config.get_id().startswith(URL_PREFIX)


def test_morphology_assignment_config__to_model(
    monkeypatch, morphology_assignment_config_distribution_location, morphology_assignment_config
):
    """Test distribution schema call."""
    _check_to_model_called(
        monkeypatch=monkeypatch,
        location=morphology_assignment_config_distribution_location,
        entity=morphology_assignment_config,
        schema_name="MModelConfigRaw",
    )


def test_canonical_morphology_model_config(canonical_morphology_model_config):
    """Test the initialized entity has a valid id."""
    assert canonical_morphology_model_config.get_id().startswith(URL_PREFIX)


def test_canonical_morphology_model_config__to_model(
    monkeypatch,
    canonical_morphology_model_config_distribution_location,
    canonical_morphology_model_config,
):
    """Test distribution schema call."""
    _check_to_model_called(
        monkeypatch=monkeypatch,
        location=canonical_morphology_model_config_distribution_location,
        entity=canonical_morphology_model_config,
        schema_name="CanonicalDistributionConfig",
    )


def test_placeholder_morphology_config(placeholder_morphology_config):
    """Test the initialized entity has a valid id."""
    assert placeholder_morphology_config.get_id().startswith(URL_PREFIX)


def test_placeholder_morphology_config__to_model(
    monkeypatch, placeholder_morphology_config_distribution_location, placeholder_morphology_config
):
    """Test distribution schema call."""
    _check_to_model_called(
        monkeypatch=monkeypatch,
        location=placeholder_morphology_config_distribution_location,
        entity=placeholder_morphology_config,
        schema_name="PlaceholderDistributionConfig",
    )

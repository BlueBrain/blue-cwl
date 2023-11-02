import json
import pytest
from pathlib import Path
from unittest.mock import patch, Mock

from cwl_registry.mmodel import entity as test_module
from cwl_registry.mmodel import schemas

from entity_management import nexus, util

URL_PREFIX = "https://bbp.epfl.ch"


def test_morphology_assignment_config(morphology_assignment_config):
    """Test that the initialized entity has a valid id."""
    assert morphology_assignment_config.get_id().startswith(URL_PREFIX)


def test_morphology_assignment_config__to_model(monkeypatch, morphology_assignment_config):
    """Test distribution schema call."""
    monkeypatch.setattr(nexus, "file_as_dict", lambda *args, **kwargs: {"data": "my-data"})

    with (
        patch("cwl_registry.mmodel.entity.validate_schema"),
        patch(f"cwl_registry.mmodel.schemas.MModelConfigRaw.from_dict") as patched,
    ):
        model = morphology_assignment_config.to_model()
        patched.assert_called_once_with({"data": "my-data"})


def test_canonical_morphology_model_config(canonical_morphology_model_config):
    """Test the initialized entity has a valid id."""
    assert canonical_morphology_model_config.get_id().startswith(URL_PREFIX)


def test_canonical_morphology_model_config__to_model(
    monkeypatch,
    canonical_morphology_model_config,
):
    """Test distribution schema call."""

    with (
        patch("cwl_registry.mmodel.entity.validate_schema"),
        patch("entity_management.nexus.file_as_dict", return_value={"data": "my-data"}),
    ):
        res = canonical_morphology_model_config.to_model()
        assert res.data == {"data": "my-data"}


def test_placeholder_morphology_config(placeholder_morphology_config):
    """Test the initialized entity has a valid id."""
    assert placeholder_morphology_config.get_id().startswith(URL_PREFIX)


def test_placeholder_morphology_config__to_model(monkeypatch, placeholder_morphology_config):
    """Test distribution schema call."""
    monkeypatch.setattr(nexus, "file_as_dict", lambda *args, **kwargs: {"data": "my-data"})

    with (
        patch("cwl_registry.mmodel.entity.validate_schema"),
        patch("entity_management.nexus.file_as_dict", return_value={"data": "my-data"}),
    ):
        res = placeholder_morphology_config.to_model()
        assert res.data == {"data": "my-data"}

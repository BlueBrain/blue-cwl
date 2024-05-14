from blue_cwl.core import validate as test_module
from blue_cwl.core.exceptions import CWLError
from unittest.mock import Mock

import pytest


def test_read_schema():
    for schema in ("commandlinetool.yml", "workflow.yml"):
        assert test_module._read_schema(schema)


def test_format_error():
    error = Mock(absolute_path=["/mypath", "foo"], message="error")
    res = test_module._format_error(error)
    assert res == "[/mypath -> foo]: error"


def test_validate_schema__error():
    with pytest.raises(CWLError, match="'cwlVersion' is a required property"):
        test_module.validate_schema(data={}, schema_name="commandlinetool.yml")

import os
import yaml
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from cwl_luigi import utils as tested
from cwl_luigi.exceptions import CWLError


def test_cwd():
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tdir:

        tdir = Path(tdir).resolve()

        with tested.cwd(tdir):
            assert os.getcwd() == str(tdir)
        assert os.getcwd() == str(cwd)


def test_read_schema():
    for schema in ("commandlinetool.yml", "workflow.yml"):
        assert tested._read_schema(schema)


def test_format_error():

    error = Mock(absolute_path=["/mypath", "foo"], message="error")
    res = tested._format_error(error)
    assert res == "[/mypath -> foo]: error"


def test_validate_schema__error():

    with pytest.raises(CWLError, match="'cwlVersion' is a required property"):
        tested.validate_schema(data={}, schema_name="commandlinetool.yml")

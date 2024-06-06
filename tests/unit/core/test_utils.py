import os
import yaml
import json
import tempfile
from pathlib import Path

from blue_cwl.core import utils as tested


def test_cwd():
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tdir:
        tdir = Path(tdir).resolve()

        with tested.cwd(tdir):
            assert os.getcwd() == str(tdir)
        assert os.getcwd() == str(cwd)


def test_create_dir(tmp_path):
    directory = Path(tmp_path / "sub")

    assert not directory.exists()

    path = tested.create_dir(directory)
    assert path == directory
    assert path.exists()

    file = path / "file.txt"
    file.write_text("foo")

    # already exists
    path = tested.create_dir(directory)
    assert path == directory
    assert path.exists()

    # check that the directory is not cleaned
    assert file.exists()
    assert file.read_text() == "foo"


def test_load_json(tmp_path):
    file = Path(tmp_path / "file.json")
    file.write_text(json.dumps({"a": "b"}))

    res = tested.load_json(file)
    assert res == {"a": "b"}


def test_write_json(tmp_path):
    file = Path(tmp_path / "file.json")
    tested.write_json(data={"a": "b"}, filepath=file)
    assert tested.load_json(file) == {"a": "b"}


def test_load_yaml(tmp_path):
    file = Path(tmp_path / "file.yml")
    file.write_text("a: b")

    res = tested.load_yaml(file)
    assert res == {"a": "b"}


def test_write_yaml(tmp_path):
    data = {"a": {"foo": "bar"}, "c": {"foo": "bar"}}

    file = Path(tmp_path / "file.json")
    tested.write_yaml(data=data, filepath=file)
    assert tested.load_yaml(file) == data

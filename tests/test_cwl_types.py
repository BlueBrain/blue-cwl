import os
import pytest
from cwl_luigi.exceptions import CWLError
from cwl_luigi import cwl_types as tested


def test_File():

    res = tested.File(path="/gpfs/foo.txt")
    assert res.path == "/gpfs/foo.txt"
    assert res.basename == "foo.txt"
    assert os.path.isabs(res.location[7:])
    assert res.location == "file:///gpfs/foo.txt"

    res = tested.File(path="foo.txt")
    assert res.path == "foo.txt"
    assert os.path.isabs(res.location[7:])
    assert res.location.endswith("foo.txt")
    assert res.basename == "foo.txt"

    res = tested.File(location="file:///gpfs/foo.txt")
    assert res.path == "/gpfs/foo.txt"
    assert res.basename == "foo.txt"


def test_Directory():

    res = tested.Directory(path="/gpfs/foo")
    assert res.path == "/gpfs/foo"
    assert res.basename == "foo"
    assert os.path.isabs(res.location[7:])
    assert res.location == "file:///gpfs/foo"

    res = tested.Directory(path="foo")
    assert res.path == "foo"
    assert os.path.isabs(res.location[7:])
    assert res.location.endswith("foo")
    assert res.basename == "foo"

    res = tested.Directory(location="file:///gpfs/foo")
    assert res.path == "/gpfs/foo"
    assert res.basename == "foo"

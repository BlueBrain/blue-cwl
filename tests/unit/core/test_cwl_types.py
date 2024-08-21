import os
import pytest
from blue_cwl.core.exceptions import CWLError
from blue_cwl.core import cwl_types as tested
from blue_cwl.utils import cwd


def test_File(tmp_path):
    """
    File fields: https://www.commonwl.org/v1.0/CommandLineTool.html#File
    """

    contents = "foo"

    path = tmp_path / "foo.txt"
    path.write_text(contents)

    parent_dir = str(tmp_path)
    path = str(path)
    uri = f"file://{path}"
    size = 3
    checksum = "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"

    res = tested.File(path=path)
    assert res.path == path
    assert res.basename == "foo.txt"
    assert res.dirname == parent_dir
    assert res.nameroot == "foo"
    assert res.nameext == ".txt"
    assert os.path.isabs(res.location[7:])
    assert res.location == uri
    assert res.size == size
    assert res.contents == contents
    assert res.checksum == checksum

    with cwd(tmp_path):
        res = tested.File(path="foo.txt")
        assert res.path == "foo.txt"
        assert res.basename == "foo.txt"
        assert res.dirname == "."
        assert res.nameroot == "foo"
        assert res.nameext == ".txt"
        assert os.path.isabs(res.location[7:])
        assert res.location == uri
        assert res.size == size
        assert res.contents == contents
        assert res.checksum == checksum

    res = tested.File(location=uri)
    assert res.path == path
    assert res.basename == "foo.txt"
    assert res.dirname == parent_dir
    assert res.nameroot == "foo"
    assert res.nameext == ".txt"
    assert os.path.isabs(res.location[7:])
    assert res.location == uri
    assert res.size == size
    assert res.contents == contents
    assert res.checksum == checksum


def test_Directory():
    """
    Directory fields: https://www.commonwl.org/v1.0/CommandLineTool.html#Directory
    """
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

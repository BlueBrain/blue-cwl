import pytest
from cwl_luigi.exceptions import CWLError
from cwl_luigi import cwl_types as tested


def test_cwltype__from_string():
    res = tested.CWLType.from_string("File")
    assert res == tested.CWLType.FILE

    with pytest.raises(CWLError, match=f"Unknown type 'John'. "):
        tested.CWLType.from_string("John")

import pytest
from cwl_luigi.core import config as test_module


def test_config__update():

    c1 = test_module.RemoteConfig(host="foo")
    c2 = test_module.RemoteConfig(host="bar")
    c3 = c1.update(c2)
    c4 = c2.update(c1)

    assert c3.host == "bar"
    assert c4.host == "foo"

    f1 = test_module.SlurmConfig()
    f2 = test_module.SlurmConfig(exclusive=True, mem=0)
    f3 = f1.update(f2)
    f4 = f2.update(f1)

    assert f3.exclusive == True
    assert f3.mem == 0

    assert f4.exclusive == False
    assert f4.mem is None

    with pytest.raises(AssertionError):
        f1.update(c1)
        c1.update(f1)


def test_SlurmConfig__default():
    config = test_module.SlurmConfig()

    res = config.to_command_parameters()

    assert res == ["--partition=prod", "--constraint=cpu"]


def test_SlurmConfig__bool():
    config = test_module.SlurmConfig(exclusive=True)

    res = config.to_command_parameters()

    assert res == ["--partition=prod", "--constraint=cpu", "--exclusive"]

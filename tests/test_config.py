from cwl_luigi import config as test_module


def test_SlurmConfig__default():
    config = test_module.SlurmConfig()

    res = config.to_command_parameters()

    assert res == ["--partition=prod", "--constraint=cpu"]


def test_SlurmConfig__bool():
    config = test_module.SlurmConfig(exclusive=True)

    res = config.to_command_parameters()

    assert res == ["--partition=prod", "--constraint=cpu", "--exclusive"]

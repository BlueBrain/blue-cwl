import inspect

from blue_cwl.wrappers import connectome_generation_placeholder as test_module


def _check_arg_consistency(cli_command, function):
    """Check that command has the same arguments as the function."""

    cmd_args = set(p.name for p in cli_command.params)
    func_args = set(inspect.signature(function).parameters.keys())

    assert cmd_args == func_args, (
        "Command arguments are not matching function ones:\n"
        f"Command args : {sorted(cmd_args)}\n"
        f"Function args: {sorted(func_args)}"
    )


def test_stage_cli():
    _check_arg_consistency(test_module.stage_cli, test_module.stage)


def test_transform_cli():
    _check_arg_consistency(test_module.transform_cli, test_module.transform)


def test_register_cli():
    _check_arg_consistency(test_module.register_cli, test_module.register)

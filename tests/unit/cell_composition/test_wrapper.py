import inspect

from blue_cwl.wrappers import cell_composition_manipulation as test_module


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


def test_manipulate_cell_composition_cli():
    _check_arg_consistency(
        test_module.manipulate_cell_composition_cli, test_module.manipulate_cell_composition
    )


def test_register_cli():
    _check_arg_consistency(test_module.register_cli, test_module.register)

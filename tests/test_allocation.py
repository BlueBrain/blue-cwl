from cwl_luigi import allocation as tested


def test_allocation__shell_command():

    config = {
        "jobname": "F",
        "salloc": "salloc -N 1 -n 2 --exclusive",
        "env_vars": {"A": "X", "B": "Y"},
    }
    allocation = tested.Allocation.from_config(config=config)
    res = allocation.shell_command(cmd="echo X")
    assert (
        res
        == "export A=X B=Y && stdbuf -oL -eL salloc -J F salloc -N 1 -n 2 --exclusive srun sh -c 'echo X'"
    )
    config["skip_srun"] = True
    allocation = tested.Allocation.from_config(config=config)
    res = allocation.shell_command(cmd="echo X")
    assert (
        res
        == "export A=X B=Y && stdbuf -oL -eL salloc -J F salloc -N 1 -n 2 --exclusive srun sh -c 'echo X'"
    )


def test_escape_single_quotes():

    value = "a/'b'/c"
    res = tested._escape_single_quotes(value)
    assert res == "a/'\\''b'\\''/c"

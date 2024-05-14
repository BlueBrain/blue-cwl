from cwl_luigi.core import environment as test_module


def test_build_environment_command():

    cfg = {
        "env_type": "VENV",
        "path": "/foo",
    }

    res = test_module.build_environment_command("cmd", cfg)

    assert res == ". /foo/bin/activate && cmd"

    cfg = {"env_type": "MODULE", "modules": ["unstable", "foo"]}

    res = test_module.build_environment_command("cmd", cfg)

    assert res == (
        ". /etc/profile.d/modules.sh && module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable foo && cmd"
    )

    cfg = {
        "env_type": "APPTAINER",
        "image": "foo",
    }

    res = test_module.build_environment_command("cmd", cfg)

    assert res == (
        ". /etc/profile.d/modules.sh && module purge && "
        "module use /gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable singularityce && singularity --version && "
        "singularity exec --cleanenv --containall "
        "--bind $TMPDIR:/tmp,/gpfs/bbp.cscs.ch/project /gpfs/bbp.cscs.ch/ssd/containers/foo "
        'bash <<EOF\ncd "$(pwd)" && cmd\nEOF\n'
    )

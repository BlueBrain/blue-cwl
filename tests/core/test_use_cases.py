import tempfile
from pathlib import Path
from cwl_luigi.core import parse_cwl_file
from cwl_luigi.core.cwl_types import Directory, CWLType, File, NexusResource
from cwl_luigi.core.utils import cwd
from cwl_luigi.core import cwl

import pytest

DATA_DIR = Path(__file__).parent / "data"
CWL_DIR = DATA_DIR / "use_cases"


def test_tool_write():

    filepath = CWL_DIR / "write.cwl"
    input_values = {"message": "Lorem-Ipsum"}

    tool = parse_cwl_file(filepath)

    with tempfile.TemporaryDirectory() as tdir:
        with cwd(tdir):
            res = tool.make(input_values=input_values)

            assert res.inputs == {"message": "Lorem-Ipsum"}

            assert res.outputs == {
                "example_file": File(path="Lorem-Ipsum_file-output.txt"),
            }

            assert res.base_command == [str(CWL_DIR / "write.py"), "Lorem-Ipsum"]

            assert res.build_command() == f"{CWL_DIR}/write.py Lorem-Ipsum"

            res.run()
            assert str(Path(tdir, "file-output.txt").read_text()) == "Lorem-Ipsum"


def test_parameter_references__tool_outputs():

    input_values = {"indir": Directory(path="foo/bar")}

    tool = parse_cwl_file(DATA_DIR / "use_cases" / "file_param_ref.cwl")

    res = tool.make(input_values=input_values)

    assert res.build_command() == "cp -r foo/bar"

    assert res.outputs == {
        "r1": File(path="bar/file.txt"),
        "r2": File(path="foo/bar/file.txt"),
        "r3": Directory(path="foo/bar/subdir"),
    }


def test_essential_parameters():

    tool = parse_cwl_file(DATA_DIR / "use_cases" / "essential_parameters.cwl")

    input_values = {
        "example_flag": True,
        "example_string": "hello",
        "example_int": 42,
        "example_file": "whale.txt",
        "example_float": 3.2,
    }
    res = tool.make(input_values=input_values)
    assert res.build_command() == "echo -f -i42 -d3.2 --file=whale.txt --example-string hello"


def test_array_types_tool():

    tool = parse_cwl_file(CWL_DIR / "array_types.cwl")

    input_values = {
        "filesA": ["a", "b", "c", "d"],
        "filesB": ["c", "d", "e", "f"],
        "filesC": ["g", "h"],
    }

    res = tool.make(input_values=input_values)
    res.build_command() == "touch foo.txt -A a b c d -C=c,d,e,f"


def test_array_types_workflow():

    tool = parse_cwl_file(DATA_DIR / "use_cases" / "array_types_workflow.cwl")

    input_values = {
        "filesA": ["a", "b", "c", "d"],
        "filesB": ["c", "d", "e", "f"],
        "one_file": "o",
    }

    res = tool.make(input_values=input_values)

    s0, s1 = res.steps

    s0.build_command() == "touch foo.txt -A a b c d -B=c -B=d -B=e -B=f -C=c_foo,c_bar"
    s1.build_command() == "touch foo.txt -A a b c d -B=foo.txt_file.txt -B=o -B=e -B=f -C=foo.txt,o"


def test_copy_file_tool(tmp_path):

    input_file = tmp_path / "copy_input_file.json"
    input_file.write_text("foo")

    output_file = tmp_path / "copy_output_file.json"

    input_values = {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "overwrite": True,
    }

    tool = parse_cwl_file(CWL_DIR / "copy_file.cwl")
    process = tool.make(input_values=input_values)

    assert process.inputs == {
        "input_file": File(path=input_file),
        "output_file": File(path=output_file),
        "overwrite": True,
    }

    assert process.outputs == {"output_file": File(path=str(output_file))}

    process.build_command() == f"python3 {CWL_DIR}/copy_file.py --overwrite {input_file} {output_file}"
    process.run()

    assert input_file.read_text() == output_file.read_text()

    input_values = {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "overwrite": False,
    }
    process = tool.make(input_values=input_values)
    process.build_command() == f"python3 {CWL_DIR}/copy_file.py {input_file} {output_file}"

    # cannot overwrite the output file
    with pytest.raises(RuntimeError):
        process.run()

    # check if default works
    input_values = {
        "input_file": str(input_file),
        "output_file": str(output_file),
    }
    process.build_command() == f"python3 {CWL_DIR}/copy_file.py {input_file} {output_file}"

    # cannot overwrite the output file
    with pytest.raises(RuntimeError):
        process.run()


@pytest.mark.parametrize(
    "cwl_file",
    [
        "copy_file_chain.cwl",
        "copy_file_chain_embedded.cwl",
    ],
)
def test_copy_file_workflow(tmp_path, cwl_file):

    workflow = parse_cwl_file(CWL_DIR / cwl_file)

    input_file = tmp_path / "copy_chain_input_file.json"
    input_file.write_text("foo")

    output_dir = tmp_path / "out"
    output_dir.mkdir()

    input_values = {
        "input_file": str(input_file),
        "output_dir": str(output_dir),
        "overwrite": True,
    }

    process = workflow.make(input_values)
    s1, s2, s3 = process.steps

    assert s1.inputs == {
        "input_file": File(path=str(input_file)),
        "output_file": File(path=str(output_dir / "s0_output_file.txt")),
        "overwrite": True,
    }
    assert s1.outputs == {
        "output_file": File(path=str(output_dir / "s0_output_file.txt")),
    }
    assert (
        s1.build_command()
        == f"python3 {CWL_DIR}/copy_file.py --overwrite {input_file} {output_dir}/s0_output_file.txt"
    )
    s1.run()

    assert s2.inputs == {
        "input_file": File(path=str(output_dir / "s0_output_file.txt")),
        "output_file": File(path=str(output_dir / "s1_output_file.txt")),
        "overwrite": True,
    }
    assert s2.outputs == {
        "output_file": File(path=str(output_dir / "s1_output_file.txt")),
    }
    assert (
        s2.build_command()
        == f"python3 {CWL_DIR}/copy_file.py --overwrite {output_dir}/s0_output_file.txt {output_dir}/s1_output_file.txt"
    )
    s2.run()

    assert s3.inputs == {
        "input_file": File(path=str(output_dir / "s1_output_file.txt")),
        "output_file": File(path=str(output_dir / "s2_output_file.txt")),
        "overwrite": True,
    }
    assert s3.outputs == {
        "output_file": File(path=str(output_dir / "s2_output_file.txt")),
    }
    assert (
        s3.build_command()
        == f"python3 {CWL_DIR}/copy_file.py --overwrite {output_dir}/s1_output_file.txt {output_dir}/s2_output_file.txt"
    )
    s3.run()

    assert Path(process.outputs["output_file"].path).read_text() == input_file.read_text()


def test_generator_tool():

    tool = parse_cwl_file(CWL_DIR / "generator_tool.cwl")

    input_values = {
        "region": "my-region",
        "cell_composition": "my-composition-id",
        "variant_config": "my-config-id",
        "output_dir": "my-output-dir",
    }

    process = tool.make(input_values)

    assert process.inputs == {
        "region": "my-region",
        "cell_composition": NexusResource(id="my-composition-id"),
        "variant_config": NexusResource(id="my-config-id"),
        "output_dir": Directory(path="my-output-dir"),
    }

    assert process.outputs == {
        "partial_circuit": NexusResource(path="partial-circuit.json"),
    }

    assert process.base_command == [
        "cwl-registry",
        "execute",
        "neurons-cell-position",
        "--region",
        "my-region",
        "--cell-composition",
        "my-composition-id",
        "--variant-config",
        "my-config-id",
        "--output-dir",
        "my-output-dir",
    ]

    assert process.environment == {
        "env_type": "MODULE",
        "modules": ["unstable", "brainbuilder", "py-cwl-registry"],
        "enable_internet": True,
    }

    assert process.executor.to_dict() == {
        "remote_config": {"host": "bbpv1.epfl.ch"},
        "slurm_config": {
            "chdir": None,
            "account": "proj134",
            "partition": "prod",
            "nodes": 1,
            "qos": None,
            "ntasks": None,
            "ntasks_per_node": None,
            "cpus_per_task": None,
            "mpi": None,
            "mem": None,
            "mem_per_cpu": None,
            "constraint": "cpu",
            "exclusive": True,
            "time": "8:00:00",
            "dependency": None,
            "job_name": None,
            "output": None,
            "array": None,
            "wait": False,
        },
        "env_vars": {"BAR": "bar", "FOO": "foo"},
    }

    assert process.build_command() == (
        ". /etc/profile.d/modules.sh && module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable brainbuilder py-cwl-registry && "
        "export FOO=foo BAR=bar && "
        "stdbuf -oL -eL salloc --account=proj134 --partition=prod --nodes=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "cwl-registry execute neurons-cell-position "
        "--region my-region "
        "--cell-composition my-composition-id "
        "--variant-config my-config-id --output-dir my-output-dir"
    )


def test_generator_workflow():

    workflow = parse_cwl_file(CWL_DIR / "generator_workflow.cwl")

    input_values = {
        "region": "my-region",
        "cell_composition": "my-composition-id",
        "variant_config": "my-config-id",
        "output_dir": "my-output-dir",
    }

    process = workflow.make(input_values)

    s1, s2, s3, s4 = process.steps

    assert s1.inputs == {
        "cell_composition": NexusResource(id="my-composition-id"),
        "output_dir": Directory(path="my-output-dir"),
    }
    assert s1.outputs == {"staged_resource": File(path="my-output-dir/staged_resource.json")}
    assert s1.base_command == [
        "cwl-registry",
        "stage",
        "cell-composition",
        "my-composition-id",
        "my-output-dir",
    ]
    assert s1.environment == {"env_type": "MODULE", "modules": ["unstable", "py-cwl-registry"]}
    assert s1.executor.to_dict() == {
        "env_vars": {"FOO": "foo"},
        "remote_config": {"host": "bbpv1.epfl.ch"},
        "slurm_config": {
            "chdir": None,
            "account": "proj134",
            "partition": "prod",
            "nodes": 1,
            "qos": None,
            "ntasks": None,
            "ntasks_per_node": None,
            "cpus_per_task": None,
            "mpi": None,
            "mem": None,
            "mem_per_cpu": None,
            "constraint": "cpu",
            "exclusive": True,
            "time": "8:00:00",
            "dependency": None,
            "job_name": None,
            "output": None,
            "array": None,
            "wait": False,
        },
    }
    assert s1.build_command() == (
        ". /etc/profile.d/modules.sh && module purge && export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable py-cwl-registry && "
        "export FOO=foo && stdbuf -oL -eL salloc --account=proj134 --partition=prod --nodes=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "cwl-registry stage cell-composition my-composition-id my-output-dir"
    )
    assert s2.inputs == {
        "variant_config": NexusResource(id="my-config-id"),
        "output_dir": Directory(path="my-output-dir"),
    }
    assert s2.outputs == {"staged_resource": File(path="my-output-dir/staged_resource.json")}
    assert s2.environment == {"env_type": "MODULE", "modules": ["unstable", "py-cwl-registry"]}
    assert s2.base_command == [
        "cwl-registry",
        "stage",
        "variant-config",
        "my-config-id",
        "my-output-dir",
    ]
    assert s2.executor.to_dict() == {
        "env_vars": None,
        "remote_config": {"host": "bbpv1.epfl.ch"},
        "slurm_config": {
            "chdir": None,
            "account": "proj134",
            "partition": "prod",
            "nodes": None,
            "qos": None,
            "ntasks": 1,
            "ntasks_per_node": None,
            "cpus_per_task": None,
            "mpi": None,
            "mem": None,
            "mem_per_cpu": None,
            "constraint": "cpu",
            "exclusive": True,
            "time": "8:00:00",
            "dependency": None,
            "job_name": None,
            "output": None,
            "array": None,
            "wait": False,
        },
    }
    assert s2.build_command() == (
        ". /etc/profile.d/modules.sh && module purge && export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable py-cwl-registry && "
        "stdbuf -oL -eL salloc --account=proj134 --partition=prod --ntasks=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "cwl-registry stage variant-config my-config-id my-output-dir"
    )
    assert s3.inputs == {
        "cell_composition": File(path="my-output-dir/staged_resource.json"),
        "variant_config": File(path="my-output-dir/staged_resource.json"),
        "output_dir": Directory(path="my-output-dir"),
    }

    assert s3.outputs == {"partial_circuit": File(path="my-output-dir/circuit.json")}
    assert s3.environment == {"env_type": "MODULE", "modules": ["unstable", "py-cwl-registry"]}
    assert s3.base_command == [
        "cwl-registry",
        "execute",
        "placement",
        "my-output-dir/staged_resource.json",
        "my-output-dir/staged_resource.json",
        "my-output-dir",
    ]
    assert s3.executor.to_dict() == {
        "env_vars": None,
        "remote_config": {"host": "bbpv1.epfl.ch"},
        "slurm_config": {
            "chdir": None,
            "account": "proj134",
            "partition": "prod",
            "nodes": None,
            "qos": None,
            "ntasks": 1,
            "ntasks_per_node": None,
            "cpus_per_task": None,
            "mpi": None,
            "mem": None,
            "mem_per_cpu": None,
            "constraint": "cpu",
            "exclusive": True,
            "time": "8:00:00",
            "dependency": None,
            "job_name": None,
            "output": None,
            "array": None,
            "wait": False,
        },
    }

    assert s3.build_command() == (
        ". /etc/profile.d/modules.sh && module purge && export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable py-cwl-registry && "
        "stdbuf -oL -eL salloc --account=proj134 --partition=prod --ntasks=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "cwl-registry execute placement my-output-dir/staged_resource.json my-output-dir/staged_resource.json my-output-dir"
    )

    assert s4.inputs == {
        "cell_composition": NexusResource(id="my-composition-id"),
        "variant_config": NexusResource(id="my-config-id"),
        "circuit_config": File(path="my-output-dir/circuit.json"),
    }
    assert s4.outputs == {"partial_circuit": NexusResource(path="circuit.json")}
    assert s4.base_command == [
        "cwl-registry",
        "register",
        "circuit",
        "--cell-composition",
        "my-composition-id",
        "--variant-config",
        "my-config-id",
        "--circuit-config",
        "my-output-dir/circuit.json",
    ]

    assert s4.executor.to_dict() == {"env_vars": None}
    assert s4.environment is None

    assert s4.build_command() == (
        "cwl-registry register circuit "
        "--cell-composition my-composition-id "
        "--variant-config my-config-id "
        "--circuit-config my-output-dir/circuit.json"
    )

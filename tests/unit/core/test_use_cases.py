import tempfile
import contextlib
from pathlib import Path
from blue_cwl.core import parse_cwl_file
from blue_cwl.core.cwl_types import Directory, CWLType, File, NexusResource
from blue_cwl.utils import cwd, write_yaml
from blue_cwl.core import cwl

import pytest

DATA_DIR = Path(__file__).parent / "data"
CWL_DIR = DATA_DIR / "use_cases"


@contextlib.contextmanager
def definition(definition: str | dict):
    with tempfile.NamedTemporaryFile(suffix=".cwl") as tfile:
        if isinstance(definition, str):
            with open(tfile.name, "w") as file:
                file.write(definition)

        else:
            write_yaml(data=definition, filepath=tfile.name)

        yield tfile.name


def test_echo_tool():
    filepath = CWL_DIR / "echo.cwl"
    input_values = {"input": "foo"}
    res = parse_cwl_file(filepath).make(input_values).run()
    res["out"] == "foo"


def test_tool_write(tmp_path):
    filepath = CWL_DIR / "write.cwl"
    input_values = {"message": "Lorem-Ipsum"}

    tool = parse_cwl_file(filepath)

    with cwd(tmp_path):
        process = tool.make(input_values=input_values)

        assert process.inputs == {"message": "Lorem-Ipsum"}
        assert process.base_command == [str(CWL_DIR / "write.py"), "Lorem-Ipsum"]
        assert process.build_command() == f"{CWL_DIR}/write.py Lorem-Ipsum"
        res = process.run()
        assert res == {"example_file": File(path="file-output.txt")}
        assert (tmp_path / "file-output.txt").read_text() == "Lorem-Ipsum"


def test_parameter_references__tool_outputs(tmp_path):
    indir = tmp_path / "indir"
    indir.mkdir()

    r1 = indir / "file1.txt"
    r1.touch()

    r2 = indir / "file2.txt"
    r2.touch()

    r3 = indir / "subdir"
    r3.mkdir()

    outdir = tmp_path / "outdir"

    input_values = {"indir": Directory(path=indir), "outdir": str(outdir)}

    tool = parse_cwl_file(DATA_DIR / "use_cases" / "file_param_ref.cwl")

    process = tool.make(input_values=input_values)

    assert process.inputs == input_values
    assert process.build_command() == f"cp -r {indir} {outdir}"

    res = process.run()

    assert res == {
        "r1": File(path=outdir / r1.name),
        "r2": File(path=outdir / r2.name),
        "r3": Directory(path=outdir / r3.name),
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
    process = tool.make(input_values=input_values)
    assert process.build_command() == "echo -f -i42 -d3.2 --file=whale.txt --example-string hello"


def test_array_types_tool():
    tool = parse_cwl_file(CWL_DIR / "array_types.cwl")

    input_values = {
        "filesA": ["a", "b", "c", "d"],
        "filesB": ["c", "d", "e", "f"],
        "filesC": ["g", "h"],
    }

    process = tool.make(input_values=input_values)
    assert process.build_command() == "touch foo.txt -A a b c d -B=c -B=d -B=e -B=f -C=g,h"


def test_array_types_tool__2():
    content = """
        cwlVersion: v1.2
        id: foo
        class: CommandLineTool
        inputs:
          files:
            type: File[]
            inputBinding:
              prefix: -A
              position: 1
              separate: true
        outputs:
          output_file:
            type: File
            outputBinding:
              glob: foo.txt
        baseCommand: touch foo.txt
    """

    with definition(content) as path:
        tool = parse_cwl_file(path)

        input_values = {
            "files": ["a", "b", "c", "d"],
        }

        # following the original implementation, when separate=True there is no separation at thi
        # level
        process = tool.make(input_values=input_values)
        assert process.build_command() == "touch foo.txt -A a b c d"


def test_array_types_tool__3():
    content = """
        cwlVersion: v1.2
        id: foo
        class: CommandLineTool
        inputs:
          files:
            type: File[]
            inputBinding:
              prefix: -A
              position: 1
              itemSeparator: ","
        outputs:
          output_file:
            type: File
            outputBinding:
              glob: foo.txt
        baseCommand: touch foo.txt
    """

    with definition(content) as path:
        tool = parse_cwl_file(path)

        input_values = {
            "files": ["a", "b", "c", "d"],
        }

        process = tool.make(input_values=input_values)
        assert process.build_command() == "touch foo.txt -A a,b,c,d"


def test_array_types_tool__4():
    content = """
        cwlVersion: v1.2
        id: foo
        class: CommandLineTool
        inputs:
          files:
            type:
              type: array
              items: File
              inputBinding:
                prefix: -B=
                separate: false
            inputBinding:
              position: 2
        outputs:
          output_file:
            type: File
            outputBinding:
              glob: foo.txt
        baseCommand: touch foo.txt
    """

    with definition(content) as path:
        tool = parse_cwl_file(path)

        input_values = {
            "files": ["a", "b", "c", "d"],
        }

        process = tool.make(input_values=input_values)
        assert process.build_command() == "touch foo.txt -B=a -B=b -B=c -B=d"


def test_array_types_workflow():
    tool = parse_cwl_file(DATA_DIR / "use_cases" / "array_types_workflow.cwl")

    input_values = {
        "filesA": ["a", "b", "c", "d"],
        "filesB": ["c", "d", "e", "f"],
        "one_file": "o",
    }

    res = tool.make(input_values=input_values)

    s0, s1 = res.steps

    assert s0.build_command() == "touch foo.txt -A a b c d -B=c -B=d -B=e -B=f -C=c_foo,c_bar"
    assert (
        s1.build_command()
        == "touch foo.txt -A a b c d -B=foo.txt -B=o -B=e.txt -B=f.txt -C=foo.txt,o"
    )


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

    assert (
        process.build_command()
        == f"python3 {CWL_DIR}/copy_file.py --overwrite {input_file} {output_file}"
    )
    outputs = process.run()
    assert outputs == {"output_file": File(path=str(output_file))}

    assert input_file.read_text() == output_file.read_text()

    input_values = {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "overwrite": False,
    }
    process = tool.make(input_values=input_values)
    assert process.build_command() == f"python3 {CWL_DIR}/copy_file.py {input_file} {output_file}"

    # cannot overwrite the output file
    with pytest.raises(RuntimeError):
        process.run()

    # check if default works
    input_values = {
        "input_file": str(input_file),
        "output_file": str(output_file),
    }
    assert process.build_command() == f"python3 {CWL_DIR}/copy_file.py {input_file} {output_file}"

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

    assert process.base_command == [
        "blue-cwl",
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
        "modules": ["unstable", "brainbuilder", "py-blue-cwl"],
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
        "module load unstable brainbuilder py-blue-cwl && "
        "export FOO=foo BAR=bar && "
        "stdbuf -oL -eL salloc --account=proj134 --partition=prod --nodes=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "blue-cwl execute neurons-cell-position "
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
        "blue-cwl",
        "stage",
        "cell-composition",
        "my-composition-id",
        "my-output-dir",
    ]
    assert s1.environment == {
        "env_type": "MODULE",
        "modules": ["unstable", "py-blue-cwl"],
    }
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
        "module load unstable py-blue-cwl && "
        "export FOO=foo && stdbuf -oL -eL salloc --account=proj134 --partition=prod --nodes=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "blue-cwl stage cell-composition my-composition-id my-output-dir"
    )
    assert s2.inputs == {
        "variant_config": NexusResource(id="my-config-id"),
        "output_dir": Directory(path="my-output-dir"),
    }
    assert s2.outputs == {"staged_resource": File(path="my-output-dir/staged_resource.json")}
    assert s2.environment == {
        "env_type": "MODULE",
        "modules": ["unstable", "py-blue-cwl"],
    }
    assert s2.base_command == [
        "blue-cwl",
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
        "module load unstable py-blue-cwl && "
        "stdbuf -oL -eL salloc --account=proj134 --partition=prod --ntasks=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "blue-cwl stage variant-config my-config-id my-output-dir"
    )
    assert s3.inputs == {
        "cell_composition": File(path="my-output-dir/staged_resource.json"),
        "variant_config": File(path="my-output-dir/staged_resource.json"),
        "output_dir": Directory(path="my-output-dir"),
    }

    assert s3.outputs == {"partial_circuit": File(path="my-output-dir/circuit.json")}
    assert s3.environment == {
        "env_type": "MODULE",
        "modules": ["unstable", "py-blue-cwl"],
    }
    assert s3.base_command == [
        "blue-cwl",
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
        "module load unstable py-blue-cwl && "
        "stdbuf -oL -eL salloc --account=proj134 --partition=prod --ntasks=1 --constraint=cpu --exclusive --time=8:00:00 srun "
        "blue-cwl execute placement my-output-dir/staged_resource.json my-output-dir/staged_resource.json my-output-dir"
    )

    assert s4.inputs == {
        "cell_composition": NexusResource(id="my-composition-id"),
        "variant_config": NexusResource(id="my-config-id"),
        "circuit_config": File(path="my-output-dir/circuit.json"),
    }
    assert s4.outputs == {"partial_circuit": NexusResource(path="circuit.json")}
    assert s4.base_command == [
        "blue-cwl",
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
        "blue-cwl register circuit "
        "--cell-composition my-composition-id "
        "--variant-config my-config-id "
        "--circuit-config my-output-dir/circuit.json"
    )

import shlex
import pytest
from tests.functional.utils import WrapperBuild


import subprocess


@pytest.fixture(scope="module")
def connectome_filtering(tmpdir_factory):
    inputs = {
        "configuration": "https://bbp.epfl.ch/neurosciencegraph/data/91f750ff-3281-4e98-8ee4-5102c6aa0090?rev=6",
        "partial-circuit": "https://bbp.epfl.ch/neurosciencegraph/data/93e2a8de-073e-4fb9-93f1-629af0fcf2a1",
        "variant-config": "https://bbp.epfl.ch/neurosciencegraph/data/8fa64bf6-90e3-491d-98ff-18552510b3d2",
        "output-dir": str(tmpdir_factory.mktemp("connectome-filtering")),
    }
    cmd = [
        "cwl-registry",
        "-vv",
        "execute",
        "connectome-filtering-synapses",
        "--configuration",
        inputs["configuration"],
        "--partial-circuit",
        inputs["partial-circuit"],
        "--variant-config",
        inputs["variant-config"],
        "--output-dir",
        inputs["output-dir"],
    ]

    cmd = " ".join(cmd)

    process = subprocess.Popen(
        ["bash", "-l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    cmd = (
        "source /etc/profile.d/modules.sh && module load unstable spykfunc parquet-converters && "
        f"salloc --account=proj9998 --partition=prod --nodes=1 --constraint=nvme --exclusive --time=0:10:00 srun --mpi=none sh -c '{cmd}'"
    )

    try:
        stdout, stderr = process.communicate(cmd.encode())
    except TimeoutExpired as exc:
        process.kill()
        process.wait()
        raise
    except:
        process.kill()
        raise

    retcode = process.poll()
    if retcode:
        print(stdout)
        print(stderr)
        raise subprocess.CalledProcessError(retcode, process.args, output=stdout, stderr=stderr)


def test_completes(connectome_filtering):
    pass

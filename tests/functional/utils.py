import os
import json
import subprocess

from click.testing import CliRunner
from blue_cwl.cli import main
import subprocess
from pathlib import Path
from blue_cwl.nexus import get_forge, get_resource
from blue_cwl.variant import Variant


def _print_details(command, inputs):
    forge = get_forge()

    input_details = {}
    for key, value in inputs.items():
        if key == "output-dir":
            input_details[key] = str(value)
        else:
            r = get_resource(forge, value)

            try:
                input_details[key] = {
                    "id": value,
                    "type": r.type,
                    "url": r._store_metadata._self,
                }
            except Exception as e:
                raise RuntimeError(f"Failed to print details for ({key}: {value}):\n{r}") from e

    details = {
        "inputs": input_details,
        "env": {
            "NEXUS_BASE": os.getenv("NEXUS_BASE"),
            "NEXUS_ORG": os.getenv("NEXUS_ORG"),
            "NEXUS_PROJ": os.getenv("NEXUS_PROJ"),
        },
    }

    print(f"Test Command:\nblue-cwl {' '.join(command)}")
    print(json.dumps(details, indent=2))


class WrapperBuild:
    def __init__(self, command, inputs, salloc_cmd=None):
        self.command = command
        self.inputs = inputs

        self.forge = get_forge()
        self._run(salloc_cmd=salloc_cmd)

    @property
    def tool_definition(self):
        variant = Variant.from_resource_id(self.forge, self.inputs["variant-config"])
        return variant.tool_definition

    @property
    def output_dir(self):
        return self.inputs["output-dir"]

    @property
    def output_file(self):
        d = self.tool_definition
        output_name = list(d.outputs)[0]
        return Path(self.output_dir, d.outputs[output_name].outputBinding["glob"])

    @property
    def output_id(self):
        data = json.loads(self.output_file.read_bytes())
        if "id" in data:
            return data["id"]

        try:
            return data["@id"]
        except KeyError as e:
            raise KeyError(f"No @id in {data}") from e

    @property
    def output(self):
        return self.forge.retrieve(self.output_id)

    def retrieve_input(self, name):
        return self.forge.retrieve(self.inputs[name])

    def _run(self, salloc_cmd=None):
        arguments = [f"--{key}={value}" for key, value in self.inputs.items()]

        full_command = self.command + arguments

        _print_details(full_command, self.inputs)

        cmd = " ".join(full_command)

        if salloc_cmd:
            cmd = salloc_cmd.format(cmd=cmd)

        print("Final Command:", cmd)

        subprocess.run(cmd, shell=True, check=True)

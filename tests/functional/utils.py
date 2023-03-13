import os
import json

from click.testing import CliRunner
from cwl_registry.cli import main
import subprocess
from pathlib import Path
from cwl_registry.nexus import get_forge
from cwl_registry.variant import Variant


def _print_details(command, inputs):
    forge = get_forge()

    input_details = {}
    for key, value in inputs.items():
        if key == "output-dir":
            input_details[key] = str(value)
        else:
            r = forge.retrieve(value, cross_bucket=True)
            input_details[key] = {
                "id": value,
                "type": r.type,
                "url": r._store_metadata._self,
            }

    details = {
        "inputs": input_details,
        "env": {
            "NEXUS_BASE": os.getenv("NEXUS_BASE"),
            "NEXUS_ORG": os.getenv("NEXUS_ORG"),
            "NEXUS_PROJ": os.getenv("NEXUS_PROJ"),
        },
    }

    print(f"Test Command:\ncwl-registry {' '.join(command)}")
    print(json.dumps(details, indent=2))


class WrapperBuild:
    def __init__(self, command, inputs):
        self.command = command
        self.inputs = inputs

        self.forge = get_forge()
        self._run()

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
        return json.loads(self.output_file.read_bytes())["id"]

    @property
    def output(self):
        return self.forge.retrieve(self.output_id)

    def retrieve_input(self, name):
        return self.forge.retrieve(self.inputs[name])

    def _run(self):
        arguments = [f"--{key}={value}" for key, value in self.inputs.items()]

        full_command = self.command + arguments

        result = CliRunner().invoke(
            main, full_command, env=os.environ, catch_exceptions=False, color=True
        )
        if result.exit_code != 0:
            _print_details(full_command, self.inputs)
        assert result.exit_code == 0, result.output

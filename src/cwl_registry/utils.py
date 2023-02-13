"""Utilities."""
import functools
import inspect
import json
import logging
import os
import pathlib
import subprocess
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

import click
import voxcell
import yaml

from cwl_registry.constants import DEFAULT_CIRCUIT_BUILD_PARAMETERS
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.nexus import get_resource

ExistingFile = click.Path(
    exists=True, readable=True, dir_okay=False, resolve_path=True, path_type=str
)
ExistingDirectory = click.Path(
    exists=True, readable=True, dir_okay=True, resolve_path=True, path_type=str
)


L = logging.getLogger()


def log(function, logger=L):
    """Log the signature of a function.

    Note: Do not use for functions that receive large inputs as it may slow down runtime.
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        signature = inspect.signature(function)

        params = [
            (k, v.default if v.default is not inspect.Parameter.empty else None)
            for k, v in signature.parameters.items()
        ]

        # create argument pairs
        arg_pairs = [(name, v) for (name, _), v in zip(params[: len(args)], args)]

        # use kwargs or defaults for the rest of the parameters
        arg_pairs.extend(
            (name, kwargs[name] if name in kwargs else default_value)
            for name, default_value in params[len(args) :]
        )

        str_v = "  " + "\n  ".join([f"{k} = {v!r}" for k, v in arg_pairs])

        str_function_repr = f" Name: {function.__name__}\n" f" Args: \n{str_v}\n"
        logger.debug("Executed function:\n%s\n", str_function_repr)

        return function(*args, **kwargs)

    return wrapper


@contextmanager
def cwd(path):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


@log
def create_dir(path: os.PathLike) -> Path:
    """Create directory and parents if it doesn't already exist."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


@log
def load_json(filepath: os.PathLike) -> Dict[Any, Any]:
    """Load from JSON file."""
    return json.loads(Path(filepath).read_bytes())


@log
def write_json(filepath: os.PathLike, data: Dict[Any, Any]) -> None:
    """Write json file."""
    with open(filepath, "w", encoding="utf-8") as fd:
        json.dump(data, fd, indent=2)


@log
def load_yaml(filepath: os.PathLike) -> Dict[Any, Any]:
    """Load from YAML file."""
    return yaml.safe_load(Path(filepath).read_bytes())


@log
def write_yaml(filepath: os.PathLike, data: dict) -> None:
    """Writes dict data to yaml."""

    class Dumper(yaml.SafeDumper):
        """Custom dumper that adds an empty line between root level entries."""

        def write_line_break(self, data=None):
            super().write_line_break(data)

            if len(self.indents) == 1:
                super().write_line_break()

    def path_representer(dumper, path):
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(path))

    Dumper.add_multi_representer(pathlib.PurePath, path_representer)

    with open(filepath, mode="w", encoding="utf-8") as out_file:
        yaml.dump(data, out_file, Dumper=Dumper, sort_keys=False, default_flow_style=False)


@log
def run_circuit_build_phase(
    *, bioname_dir: Path, cluster_config_file: Path, phase: str, output_dir: Path
):
    """Execute a circuit-build phase."""
    cmd = [
        "circuit-build",
        "run",
        "--bioname",
        str(bioname_dir),
        "--cluster-config",
        str(cluster_config_file),
        phase,
    ]

    env = os.environ.copy()
    env["ISOLATED_PHASE"] = "True"

    L.debug("Command: %s", " ".join(cmd))

    subprocess.run(cmd, cwd=str(output_dir), env=env, check=True, capture_output=False)


@log
def build_manifest(
    *,
    region: str,
    atlas_dir: Path,
    morphology_release_dir: Optional[Path] = None,
    synthesis: bool = False,
    parameters: Optional[Dict[str, Any]] = None,
) -> dict:
    """Build MANIFEST.yaml for circuit-build build."""

    def optional(entry):
        return "" if entry is None else entry

    return {
        "common": {
            "atlas": atlas_dir,
            "region": region,
            "node_population_name": f"{region}_neurons",
            "edge_population_name": f"{region}_neurons__chemical_synapse",
            "morph_release": optional(morphology_release_dir),
            "synthesis": synthesis,
            "partition": ["left", "right"],
        },
        **(parameters or DEFAULT_CIRCUIT_BUILD_PARAMETERS),
    }


def get_config_path_from_circuit_resource(forge, resource_id: str) -> Path:
    """Get config path from resource.

    Note:
        It supports the following representations of circuitConfigPath:
            - A single string with or without a file prefix.
            - A DataDownload resource with the config path as a url with or without file prefix.
    """
    partial_circuit_resource = get_resource(forge, resource_id)

    config_path = partial_circuit_resource.circuitConfigPath

    # DataDownload resource with a url
    try:
        path = config_path.url
    # A single string
    except AttributeError:
        path = config_path

    if path.startswith("file://"):
        return Path(path[7:])
    return Path(path)


def get_biophysical_partial_population_from_config(circuit_config):
    """Get the biophysical population file and name fromt he config."""
    nodes_file = population_name = None
    for node_dict in circuit_config["networks"]["nodes"]:
        populations = node_dict["populations"]
        assert len(populations) == 1
        population_name = next(iter(populations))
        if populations[population_name]["type"] == "biophysical":
            nodes_file = node_dict["nodes_file"]
            break

    if nodes_file is None or population_name is None:
        raise CWLWorkflowError(f"No biophysical population found in config: {circuit_config}")

    return nodes_file, population_name


def update_circuit_config_population(
    config: Dict[str, Any],
    population_name: str,
    population_data: Dict[str, Any],
    filepath: os.PathLike,
) -> Dict[str, Any]:
    """Create a new config from an existing one with updated population data."""
    config = deepcopy(config)

    network_entries = (
        entry for network_data in config["networks"].values() for entry in network_data
    )

    for entry in network_entries:
        if population_name in entry["populations"]:
            entry["nodes_file"] = str(filepath)
            entry["populations"][population_name].update(population_data)
            return config

    raise CWLWorkflowError(f"Population name {population_name} not in config.")


def write_node_population_with_properties(
    nodes_file: Path, population_name: str, properties: Dict[str, Any], output_file: Path
):
    """Write a copy of nodes_file with additional properties for the given population."""
    population = voxcell.CellCollection.load_sonata(nodes_file, population_name=population_name)
    population.add_properties(properties, overwrite=False)
    population.save_sonata(output_file)


def get_directory_contents(directory_path: Path) -> Dict[str, Path]:
    """Return the file in a dictionary if it exists, an empty dict otherwise."""
    if directory_path.is_dir():
        return {path.name: path for path in directory_path.iterdir()}
    return {}


def get_region_resource_acronym(forge, resource_id: str) -> str:
    """Retrieve the hierarchy acronym from a KG registered region."""
    return get_resource(forge, resource_id).notation


def write_resource_to_definition_output(
    forge, resource, variant, output_dir: os.PathLike, output_name: str = None
):
    """Write a resource to the filepath determined by the tool output path definition.

    Note: This function assumes there is only one output if 'output_name' is None
    """
    outputs = variant.tool_definition.outputs

    if not output_name:
        assert len(outputs) == 1
        output_name = list(outputs)[0]

    out_filename = outputs[output_name].outputBinding["glob"]

    out_filepath = Path(output_dir, out_filename)

    write_json(filepath=out_filepath, data=forge.as_json(resource))

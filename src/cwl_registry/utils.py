"""Utilities."""

import functools
import inspect
import json
import logging
import os
import pathlib
import shutil
import subprocess
import urllib
from collections.abc import Sequence
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import libsonata
import numpy as np
import pandas as pd
import pyarrow
import pyarrow.fs
import voxcell
import yaml
from entity_management.core import Entity
from entity_management.util import get_entity
from pyarrow.pandas_compat import get_logical_type, get_logical_type_map

from cwl_registry.constants import DEFAULT_CIRCUIT_BUILD_PARAMETERS
from cwl_registry.exceptions import CWLWorkflowError
from cwl_registry.model import CustomBaseModel
from cwl_registry.typing import StrOrPath

if TYPE_CHECKING:
    from cwl_registry.variant import Variant

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
        arg_pairs = [(name, v) for (name, _), v in zip(params[: len(args)], args, strict=True)]

        # use kwargs or defaults for the rest of the parameters
        arg_pairs.extend(
            (name, kwargs[name] if name in kwargs else default_value)
            for name, default_value in params[len(args) :]
        )

        str_v = "  " + "\n  ".join([f"{k} = {v!r}" for k, v in arg_pairs])

        str_function_repr = f" Name: {function.__name__}\n" f" Args: \n{str_v}\n"
        logger.debug("Executed function:\n%s\n", str_function_repr)

        res = function(*args, **kwargs)

        return res

    return wrapper


@contextmanager
def cwd(path: StrOrPath):
    """Context manager to temporarily change the working directory."""
    original_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


def create_dir(path: StrOrPath, clean_if_exists=False) -> Path:
    """Create directory and parents if it doesn't already exist."""
    path = Path(path)
    if path.exists() and clean_if_exists:
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_json(filepath: StrOrPath) -> dict:
    """Load from JSON file."""
    return json.loads(Path(filepath).read_bytes())


def write_json(filepath: StrOrPath, data: dict) -> None:
    """Write json file."""

    def serializer(obj):
        """Serialize pydantic models if they are nested inside dicts."""
        if isinstance(obj, CustomBaseModel):
            return obj.to_dict()
        if isinstance(obj, Path):
            return str(obj)
        raise TypeError(f"Unexpected type {obj.__class__.__name__}")

    with open(filepath, "w", encoding="utf-8") as fd:
        json.dump(data, fd, indent=2, default=serializer)


def load_yaml(filepath: StrOrPath) -> dict:
    """Load from YAML file."""
    return yaml.safe_load(Path(filepath).read_bytes())


def dump_yaml(data: dict) -> str:
    """Serialize a dictionary into a yaml string."""

    class Dumper(yaml.SafeDumper):
        """Custom dumper that adds an empty line between root level entries."""

        def write_line_break(self, data=None):
            super().write_line_break(data)

            if len(self.indents) == 1:
                super().write_line_break()

    def path_representer(dumper, path):
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(path))

    Dumper.add_multi_representer(pathlib.PurePath, path_representer)

    return yaml.dump(data, Dumper=Dumper, sort_keys=False, default_flow_style=False)


def write_yaml(filepath: StrOrPath, data: dict) -> None:
    """Writes dict data to yaml."""
    Path(filepath).write_text(dump_yaml(data), encoding="utf-8")


def load_arrow(filepath: StrOrPath) -> pd.DataFrame:
    """Load an arrow file as a pandas dataframe."""
    with pyarrow.ipc.open_file(str(filepath)) as reader:
        try:
            return reader.read_pandas()
        except pyarrow.lib.ArrowKeyError as e:
            # pyarrow has trouble reading an empty file with dictionaries and no metadata
            # we need to manually construct an empty dataframe from the schema info
            schema = reader.schema

            types = get_logical_type_map().values()

            data_dict = {}
            for name, arrow_dtype in zip(schema.names, schema.types, strict=True):
                dtype = get_logical_type(arrow_dtype)

                if dtype in types:
                    series = pd.Series(dtype=dtype)
                elif dtype == "categorical":
                    value_type = arrow_dtype.value_type
                    series = pd.Categorical(
                        values=pd.Series(dtype=get_logical_type(value_type)),
                        ordered=arrow_dtype.ordered,
                    )
                else:
                    raise NotImplementedError(
                        f"Constructing an empty dataframe from '{dtype}' is not supported."
                    ) from e

                data_dict[name] = series
            return pd.DataFrame(data_dict)


def write_arrow(filepath: StrOrPath, dataframe: pd.DataFrame, index: bool = False) -> None:
    """Write dataframe as an arrow file."""
    table = pyarrow.Table.from_pandas(dataframe, preserve_index=index)

    with pyarrow.fs.LocalFileSystem().open_output_stream(str(filepath)) as fd:
        with pyarrow.RecordBatchFileWriter(fd, table.schema) as writer:
            writer.write_table(table)


def write_parquet(
    filepath: StrOrPath,
    dataframe: pd.DataFrame,
    index: bool = False,
    compression: str | None = "gzip",
) -> None:
    """Write pandas dataframe as an arrow file with gzip compression."""
    dataframe.to_parquet(path=filepath, index=index, engine="pyarrow", compression=compression)


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
    morphology_release_dir: Path | None = None,
    synthesis: bool = False,
    parameters: dict[str, Any] | None = None,
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


def get_biophysical_partial_population_from_config(circuit_config):
    """Get the biophysical node population file and name fromt he config."""
    nodes_file = population_name = None
    for node_dict in circuit_config["networks"]["nodes"]:
        populations = node_dict["populations"]
        if len(populations) != 1:
            raise CWLWorkflowError(f"Multiple populations encountered in node dict: {node_dict}")
        population_name = next(iter(populations))
        if populations[population_name]["type"] == "biophysical":
            nodes_file = node_dict["nodes_file"]
            break

    if nodes_file is None or population_name is None:
        raise CWLWorkflowError(f"No biophysical population found in config: {circuit_config}")

    return nodes_file, population_name


def get_first_edge_population_from_config(circuit_config: dict) -> tuple[str, str]:
    """Return first edges file and population name from config."""
    edges = circuit_config["networks"]["edges"]

    if len(edges) != 1:
        raise CWLWorkflowError(f"Expected a single edges file. Got {len(edges)}")

    edges_file = edges[0]["edges_file"]
    populations = edges[0]["populations"]

    if len(populations) != 1:
        raise CWLWorkflowError(f"Expected a single edge population. Got {len(populations)}")

    edge_population_name = next(iter(populations))

    return edges_file, edge_population_name


def get_edge_population_name(edges_file: StrOrPath) -> str:
    """Return population name from file."""
    storage = libsonata.EdgeStorage(edges_file)
    pop_names = storage.population_names
    if len(pop_names) > 1:
        raise CWLWorkflowError(
            f"More than one population are not supported.\n"
            f"Populations: {pop_names}\n"
            f"File: {edges_file}"
        )
    return list(pop_names)[0]


def update_circuit_config_population(
    config: dict[str, Any],
    population_name: str,
    population_data: dict[str, Any],
    filepath: StrOrPath,
) -> dict[str, Any]:
    """Create a new config from an existing one with updated population data."""
    config = deepcopy(config)
    population_data = deepcopy(population_data)

    network_entries = (
        entry for network_data in config["networks"].values() for entry in network_data
    )

    for entry in network_entries:
        if population_name in entry["populations"]:
            entry["nodes_file"] = str(filepath)

            existing_data = entry["populations"][population_name]

            # append the new partial entries to the existing ones
            if "partial" in population_data and "partial" in existing_data:
                partial = population_data.pop("partial")
                for e in partial:
                    if e in existing_data["partial"]:
                        raise CWLWorkflowError(f"{e} partial entry already exists.")
                existing_data["partial"].extend(partial)

            existing_data.update(population_data)

            return config

    raise CWLWorkflowError(f"Population name {population_name} not in config.")


def write_node_population_with_properties(
    nodes_file: StrOrPath,
    population_name: str,
    properties: dict[str, Any],
    output_file: StrOrPath,
    orientations: np.ndarray | None = None,
):
    """Write a copy of nodes_file with additional properties for the given population."""
    population = voxcell.CellCollection.load_sonata(nodes_file, population_name=population_name)
    population.add_properties(properties, overwrite=False)
    if orientations is not None:
        population.orientations = orientations
        population.orientation_format = "quaternions"
    population.save_sonata(output_file)


def get_directory_contents(directory_path: StrOrPath) -> dict[str, Path]:
    """Return the file in a dictionary if it exists, an empty dict otherwise."""
    directory_path = Path(directory_path)
    if directory_path.is_dir():
        return {path.name: path for path in directory_path.iterdir()}
    return {}


def write_resource_to_definition_output(
    json_resource, variant, output_dir: StrOrPath, output_name: str | None = None
):
    """Write a resource to the filepath determined by the tool output path definition.

    Note: This function assumes there is only one output if 'output_name' is None
    """
    outputs = variant.tool_definition.outputs

    if not output_name:
        if len(outputs) != 1:
            raise CWLWorkflowError("More than 1 workflow outputs found.")
        output_name = list(outputs)[0]

    output_binding = outputs[output_name].outputBinding

    if hasattr(output_binding, "glob"):
        out_filename = output_binding.glob
    else:
        out_filename = output_binding["glob"]

    # worarkound to work with $(inputs.output_dir.path)/file
    out_filepath = Path(output_dir, Path(out_filename).name)

    write_json(filepath=out_filepath, data=json_resource)


def url_without_revision(url: str) -> str:
    """Return the url without the revision query."""
    parse_result = urllib.parse.urlparse(url)
    return parse_result._replace(query="").geturl()


def url_with_revision(url: str, rev: str | None) -> str:
    """Return the url with revision.

    Args:
        url: The url string.
        rev: Optional revision number. Default is None.

    Returns:
        The url with revision if rev is not None, the url as is otherwise.
    """
    if rev is None:
        return url

    if "?rev=" in url:
        raise CWLWorkflowError(f"URL {url} has already a revision.")

    return f"{url}?rev={rev}"


def _cell_collection_from_frame(
    df: pd.DataFrame, population_name: str, orientation_format: str
) -> voxcell.CellCollection:
    # CellCollection.from_dataframe needs the index to start at 1
    df = df.reset_index(drop=True)
    df.index += 1

    cells = voxcell.CellCollection.from_dataframe(df)
    cells.population_name = population_name
    cells.orientation_format = orientation_format

    return cells


def bisect_cell_collection_by_properties(
    cell_collection: voxcell.CellCollection,
    properties: dict[str, list[str]],
) -> tuple[voxcell.CellCollection | None, voxcell.CellCollection | None]:
    """Split cell collection in two based on properties mask.

    The mask to split is constructed in two steps:
        1. For each property find the union of rows that match the property values.
        2. Intersect all property masks from (1).

    Args:
        cell_collection: The cells collection to split.
        properties:
            A dictionary the keys of which are property names and the values are property values.
        nodes_file: Path to node file.
        node_population_name: Name of node population.

    Note: Example properties dictionary:
        {
            'mtype': ['L3_TPC:A', 'L23_SBC'],
            'region': ['SSp-bfd3', "CA3"]
        }

    Returns:
        A tuple with two elements of type SplitCellCollectionInfo or None.
    """

    def split(df: pd.DataFrame, mask: np.ndarray) -> voxcell.CellCollection | None:
        """Create a CellCollection from the masked dataframe.

        Returns:
            A tuple of the selected CellCollection and the reverse indices.
        """
        if not mask.any():
            return None

        if mask.all():
            return cell_collection

        # store split node indices to reconstruct later
        masked_df = df[mask].reset_index(names="split_index", drop=False)

        cells = _cell_collection_from_frame(
            df=masked_df,
            population_name=cell_collection.population_name,
            orientation_format=cell_collection.orientation_format,
        )
        return cells

    # reset index because dataframe starts at 1
    df = cell_collection.as_dataframe().reset_index(drop=True)

    if isinstance(properties, pd.DataFrame):
        mask = (
            df[properties.columns].merge(properties, how="outer", indicator=True)["_merge"]
            == "both"
        )
    else:
        mask = np.logical_and.reduce(
            [df[name].isin(values).values for name, values in properties.items()]
        )

    return split(df, mask), split(df, ~mask)


def merge_cell_collections(
    splits: Sequence[voxcell.CellCollection | None],
    population_name: str,
    orientation_format: str = "quaternions",
) -> voxcell.CellCollection:
    """Merge cell collections using their 'split_index' column."""
    filtered: list[voxcell.CellCollection] = list(filter(lambda s: s is not None, splits))

    if len(filtered) == 1:
        return filtered[0].cells

    dataframes = [split.as_dataframe().set_index("split_index") for split in filtered]

    result = pd.concat(dataframes, ignore_index=False, join="outer").sort_index()
    return _cell_collection_from_frame(result, population_name, orientation_format)


def _parse_slurm_config(config: dict) -> list[str]:
    """Parse slurm config."""
    parameters = []
    for key, value in config.items():
        if value is None:
            continue
        key = key.replace("_", "-")
        if isinstance(value, bool):
            if value:
                param = f"--{key}" if isinstance(value, bool) and value else f"--{key}={value}"
            else:
                continue
        else:
            param = f"--{key}={value}"
        parameters.append(param)
    return parameters


def _get_variant_resources_config(variant: "Variant", sub_task_index: int | None = None) -> dict:
    """Return variant resources config."""
    resources_dict = variant.get_content().get("resources", {})

    if sub_task_index is None:
        resources = resources_dict["default"]
        L.debug("Default resources selected for variant %s: %s", variant, resources)
    else:
        resources = resources_dict["sub-tasks"][sub_task_index]
        L.debug(
            "Sub-task resources for variant %s and sub-task index %d selected: %s",
            variant,
            sub_task_index,
            resources,
        )

    return resources


def build_variant_allocation_command(
    cmd: str, variant: "Variant", sub_task_index: int | None = None, srun: str = "srun"
) -> str:
    """Construct an allocation command based on variant resource default definition."""
    slurm_config = _get_variant_resources_config(variant, sub_task_index=sub_task_index)

    env_vars = variant.tool_definition.environment.get("env_vars", {})

    if env_vars:
        str_env_vars = " ".join(f"{k}={v}" for k, v in env_vars.items())
        srun_cmd = f"env {str_env_vars} {cmd}"
    else:
        srun_cmd = f"{cmd}"

    str_slurm_parameters = " ".join(_parse_slurm_config(slurm_config))
    command = f"stdbuf -oL -eL salloc {str_slurm_parameters} {srun} {srun_cmd}"
    return command


def get_morphologies_dir(circuit_config, population_name, ext):
    """Get the morphologies directory from the circuit config."""
    pop_dict = _get_population(circuit_config["networks"]["nodes"], population_name)

    if ext == "swc":
        return pop_dict["morphologies_dir"]

    if ext == "h5":
        return pop_dict["alternate_morphologies"]["h5v1"]

    if ext == "asc":
        return pop_dict["alternate_morphologies"]["neurolucida-asc"]

    raise CWLWorkflowError(f"Unknown extension {ext}. Supported: (swc, h5, asc)")


def _get_population(node_list, population_name):
    for node_dict in node_list:
        populations = node_dict["populations"]
        if len(populations) != 1:
            raise CWLWorkflowError(f"Multiple populations encountered in node dict: {node_dict}")
        if population_name in populations:
            return populations[population_name]

    raise CWLWorkflowError(
        f"Population {population_name} does not exist in circuit config's node list: {node_list}."
    )


def get_partial_circuit_region_id(partial_circuit) -> str:
    """Return brain region id."""
    url = partial_circuit.brainLocation.brainRegion.url
    return url.replace("mba:", "http://api.brain-map.org/api/v2/data/Structure/")


def get_obj(
    obj: str | Entity,
    *,
    cls=Entity,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Helper to retrieve entity if an id is passed or return input object."""
    if isinstance(obj, str):
        return get_entity(resource_id=obj, cls=cls, base=base, org=org, proj=proj, token=token)
    return obj

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import click
import libsonata
import numpy as np
import pandas as pd
import pyarrow
import pyarrow.fs
import voxcell
import yaml
from pydantic import BaseModel

from cwl_registry.constants import DEFAULT_CIRCUIT_BUILD_PARAMETERS
from cwl_registry.exceptions import CWLWorkflowError

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

        res = function(*args, **kwargs)

        return res

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
def create_dir(path: os.PathLike, clean_if_exists=False) -> Path:
    """Create directory and parents if it doesn't already exist."""
    path = Path(path)
    if path.exists() and clean_if_exists:
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


@log
def load_json(filepath: os.PathLike) -> Dict[Any, Any]:
    """Load from JSON file."""
    return json.loads(Path(filepath).read_bytes())


def write_json(filepath: os.PathLike, data: Dict[Any, Any]) -> None:
    """Write json file."""

    def serializer(obj):
        """Serialize pydantic models if they are nested inside dicts."""
        if isinstance(obj, BaseModel):
            return obj.dict()
        if isinstance(obj, Path):
            return str(obj)
        raise TypeError(f"Unexpected type {obj.__class__.__name__}")

    with open(filepath, "w", encoding="utf-8") as fd:
        json.dump(data, fd, indent=2, default=serializer)


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
def load_arrow(filepath: os.PathLike) -> pd.DataFrame:
    """Load an arrow file as a pandas dataframe."""
    with pyarrow.fs.LocalFileSystem().open_input_file(str(filepath)) as fd:
        return pyarrow.RecordBatchFileReader(fd).read_pandas()


@log
def write_arrow(filepath: os.PathLike, dataframe: pd.DataFrame, index: bool = False) -> None:
    """Write dataframe as an arrow file."""
    table = pyarrow.Table.from_pandas(dataframe, preserve_index=index)

    with pyarrow.fs.LocalFileSystem().open_output_stream(str(filepath)) as fd:
        with pyarrow.RecordBatchFileWriter(fd, table.schema) as writer:
            writer.write_table(table)


@log
def write_parquet(
    filepath: os.PathLike, dataframe: pd.DataFrame, index: bool = False, compression="gzip"
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


def get_biophysical_partial_population_from_config(circuit_config):
    """Get the biophysical node population file and name fromt he config."""
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


def get_first_edge_population_from_config(circuit_config):
    """Return first edges file and population name from config."""
    edges = circuit_config["networks"]["edges"]

    assert len(edges) == 1, f"Expected a single edges file. Got {len(edges)}"

    edges_file = edges[0]["edges_file"]

    populations = edges[0]["populations"]

    assert len(populations) == 1, f"Expected a signle edge population. Got {len(populations)}"

    edge_population_name = next(iter(populations))

    return edges_file, edge_population_name


def get_edge_population_name(edges_file):
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
    config: Dict[str, Any],
    population_name: str,
    population_data: Dict[str, Any],
    filepath: os.PathLike,
) -> Dict[str, Any]:
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
                    assert e not in existing_data["partial"], f"{e} partial entry already exists."

                existing_data["partial"].extend(partial)

            existing_data.update(population_data)

            return config

    raise CWLWorkflowError(f"Population name {population_name} not in config.")


def write_node_population_with_properties(
    nodes_file: Path,
    population_name: str,
    properties: Dict[str, Any],
    output_file: Path,
    orientations: Optional[np.array] = None,
):
    """Write a copy of nodes_file with additional properties for the given population."""
    population = voxcell.CellCollection.load_sonata(nodes_file, population_name=population_name)
    population.add_properties(properties, overwrite=False)
    if orientations is not None:
        population.orientations = orientations
        population.orientation_format = "quaternions"
    population.save_sonata(output_file)


def get_directory_contents(directory_path: Path) -> Dict[str, Path]:
    """Return the file in a dictionary if it exists, an empty dict otherwise."""
    if directory_path.is_dir():
        return {path.name: path for path in directory_path.iterdir()}
    return {}


def write_resource_to_definition_output(
    json_resource, variant, output_dir: os.PathLike, output_name: str = None
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

    write_json(filepath=out_filepath, data=json_resource)


@log
def url_without_revision(url: str) -> str:
    """Return the url without the revision query."""
    url = urllib.parse.urlparse(url)
    return url._replace(query="").geturl()


@log
def url_with_revision(url: str, rev: Optional[str]) -> str:
    """Return the url with revision.

    Args:
        url: The url string.
        rev: Optional revision number. Default is None.

    Returns:
        The url with revision if rev is not None, the url as is otherwise.
    """
    if rev is None:
        return url

    assert "?rev=" not in url

    return f"{url}?rev={rev}"


def _cell_collection_from_frame(df: pd.DataFrame, population_name: str, orientation_format: str):
    # CellCollection.from_dataframe needs the index to start at 1
    df = df.reset_index(drop=True)
    df.index += 1

    cells = voxcell.CellCollection.from_dataframe(df)
    cells.population_name = population_name
    cells.orientation_format = orientation_format

    return cells


@dataclass
class SplitCollectionInfo:
    """Dataclass for CellCollection splits."""

    cells: voxcell.CellCollection
    reverse_indices: np.ndarray

    def as_dataframe(self) -> pd.DataFrame:
        """Return split as dataframe."""
        return self.cells.as_dataframe().set_index(self.reverse_indices)


def bisect_cell_collection_by_properties(
    cell_collection: voxcell.CellCollection,
    properties: dict[str, list[str]],
) -> list[SplitCollectionInfo | None]:
    """Split cell collection in two based on properties mask.

    The mask to split is constructed in two steps:
        1. For each property find the union of rows that match the property values.
        2. Intersect all property masks from (1).

    Args:
        cell_collection: The cells collection to split.
        properties:
            A dictionary the keys of which are property names and the values are property values.
            Example:
                {
                    'mtype': ['L3_TPC:A', 'L23_SBC'],
                    'region': ['SSp-bfd3', "CA3"]
                }

        nodes_file: Path to node file.
        node_population_name: Name of node population.

    Returns:
        A tuple with two elements of type SplitCellCollectionInfo or None.
    """

    def split(df: pd.DataFrame, mask: np.ndarray) -> SplitCollectionInfo | None:
        """Create a CellCollection from the masked dataframe.

        Returns:
            A tuple of the selected CellCollection and the reverse indices.
        """
        if not mask.any():
            return None

        masked_df = df[mask]

        cells = _cell_collection_from_frame(
            df=masked_df,
            population_name=cell_collection.population_name,
            orientation_format=cell_collection.orientation_format,
        )
        return SplitCollectionInfo(
            cells=cells,
            reverse_indices=masked_df.index.values,
        )

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
    splits: Sequence[SplitCollectionInfo | None],
    population_name: str,
    orientation_format: str = "quaternions",
) -> voxcell.CellCollection:
    """Merge cell collections using their reverse indices."""
    filtered = list(filter(lambda s: s is not None, splits))

    if len(filtered) == 1:
        return filtered[0].cells

    dataframes = [split.as_dataframe() for split in splits]

    result = pd.concat(dataframes, ignore_index=False, join="outer").sort_index()
    return _cell_collection_from_frame(result, population_name, orientation_format)


def parse_salloc_config(config):
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

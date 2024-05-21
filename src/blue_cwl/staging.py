# SPDX-License-Identifier: Apache-2.0

"""Staging utils."""

import logging
import os
import shutil
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path
from typing import Any

import pandas as pd
from entity_management.atlas import AtlasRelease, CellCompositionSummary, CellCompositionVolume
from entity_management.config import MacroConnectomeConfig, MicroConnectomeConfig, SynapseConfig
from entity_management.core import Entity
from entity_management.nexus import load_by_id
from entity_management.simulation import DetailedCircuit
from entity_management.util import unquote_uri_path

from blue_cwl import utils
from blue_cwl.exceptions import CWLWorkflowError
from blue_cwl.nexus import (
    download_distribution,
    forge_to_config,
    get_distribution,
    get_distribution_as_dict,
    get_distribution_location_path,
)
from blue_cwl.typing import IdOrEntity, StrOrPath
from blue_cwl.utils import get_obj
from blue_cwl.validation import validate_schema
from blue_cwl.variant import Variant

L = logging.getLogger(__name__)


def stage_distribution_file(
    id_or_entity: IdOrEntity,
    output_dir: StrOrPath,
    *,
    encoding_format: str | None = None,
    filename: str | None = None,
    symbolic: bool = True,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> Path:
    """Stage a distribution file from a NEXUS resource.

    Args:
        id_or_entity: Nexus id of the entity or the entity itself.
        output_dir: The output directory to copy the file or store the link.
        encoding_format: The encoding format of the distribution. Example: application/json
        filename: Filename to use. If None the source's filename will be used.
        symbolic: Whether to make a symbolic link or copy the file.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        Path to the staged file.

    Note:
        A resource may have many distributions with a different encoding format. If that's the case
        the encoding format argument is mandatory to select the respective distribution.
    """
    distribution = get_distribution(
        id_or_entity,
        encoding_format=encoding_format,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    try:
        source_file = Path(distribution.get_location_path(use_auth=token))
        target_file = Path(output_dir, filename) if filename else Path(output_dir, source_file.name)
        stage_file(
            source=source_file,
            target=target_file,
            symbolic=symbolic,
        )
    except Exception:  # pylint: disable=broad-exception-caught
        target_file = distribution.download(
            path=output_dir,
            file_name=filename,
            use_auth=token,
        )
    target_file = Path(target_file)

    if not target_file.exists():
        raise CWLWorkflowError(f"File failed to be staged at {target_file}")

    return target_file


def materialize_cell_composition_volume(
    obj: dict | IdOrEntity,
    *,
    output_file: StrOrPath | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> pd.DataFrame:
    """Materializa a cell composition volume distribution.

    Args:
        obj: One of the following:
            - Resource nexus id of entity to get distribution from1
            - Dictiorary of the data
            - Entity to get distribution from
        output_file: Optional output file to write the dataframe.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        DataFrame with the following columns:
            - mtype
            - mtype_url
            - etype
            - etype_url
            - path
    """
    dataset = _get_data(obj, cls=CellCompositionVolume, base=base, org=org, proj=proj, token=token)

    validate_schema(data=dataset, schema_name="cell_composition_volume_distribution.yml")

    @transform_cached
    def _materialize_me_type_density(entry_id, _):
        return get_distribution_location_path(entry_id, base=base, org=org, proj=proj, token=token)

    get_label = partial(
        get_entry_property, property_name="label", base=base, org=org, proj=proj, token=token
    )
    levels = (
        get_label,
        get_label,
        _materialize_me_type_density,
    )
    transformed = transform_nested_dataset(dataset, levels)

    result = pd.DataFrame(
        [
            (
                mtype_data["label"],
                etype_data["label"],
                mtype_url,
                etype_url,
                list(etype_data["hasPart"].values())[0],
            )
            for mtype_url, mtype_data in transformed["hasPart"].items()
            for etype_url, etype_data in mtype_data["hasPart"].items()
        ],
        columns=["mtype", "etype", "mtype_url", "etype_url", "path"],
    )

    if output_file:
        result.to_parquet(path=output_file)

    return result


def materialize_density_distribution(forge, dataset, output_file: StrOrPath):
    """Bacwards compatible function using forge."""
    base, org, proj, token = forge_to_config(forge)
    return materialize_cell_composition_volume(
        dataset, output_file=output_file, base=base, org=org, proj=proj, token=token
    )


def materialize_cell_composition_summary(
    obj: dict | IdOrEntity,
    *,
    output_file: StrOrPath | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> pd.DataFrame:
    """Materialize a cell composition summary distribution.

    Args:
        obj: One of the following:
            - Resource nexus id of entity to get distribution from1
            - Dictiorary of the data
            - Entity to get distribution from
        output_file: Optional output file to write the dataframe.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        DataFrame with the following columns:
            - region
            - region_url
            - region_label
            - mtype
            - mtype_url
            - etype
            - etype_url
            - density
    """
    dataset = _get_data(obj, cls=CellCompositionSummary, base=base, org=org, proj=proj, token=token)

    validate_schema(data=dataset, schema_name="cell_composition_summary_distribution.yml")

    get_notation = partial(
        get_entry_property, property_name="notation", base=base, org=org, proj=proj, token=token
    )

    rows = []
    for region_url, region_data in dataset["hasPart"].items():
        region_label = region_data["label"]
        region_notation = get_notation(region_url, region_data)["notation"]
        for mtype_url, mtype_data in region_data["hasPart"].items():
            mtype_label = mtype_data["label"]
            for etype_url, etype_data in mtype_data["hasPart"].items():
                etype_label = etype_data["label"]
                cell_density = etype_data["composition"]["neuron"]["density"]
                rows.append(
                    (
                        region_notation,
                        region_url,
                        region_label,
                        mtype_label,
                        mtype_url,
                        etype_label,
                        etype_url,
                        cell_density,
                    )
                )

    result = pd.DataFrame(
        rows,
        columns=[
            "region",
            "region_url",
            "region_label",
            "mtype",
            "mtype_url",
            "etype",
            "etype_url",
            "density",
        ],
    )

    if output_file:
        result.to_parquet(path=output_file)

    return result


def get_entry_id(entry: dict) -> str:
    """Get a NEXUS resource id from a dictionary entry.

    This function makes sure that both the id and the revision are fetched and combined to create
    the full resource's id with revision.

    Args:
        entry: The entry dictionary to extract the id with revision from.

    Returns:
        Full resource id with revision.
    """
    if "@id" in entry:
        resource_id = entry["@id"]
    else:
        resource_id = entry["id"]

    if "_rev" in entry:
        rev = entry["_rev"]
    elif "rev" in entry:
        rev = entry["rev"]
    else:
        rev = None

    if rev:
        if "?rev=" in resource_id:
            raise CWLWorkflowError(f"ID {resource_id} has already a revision.")
        resource_id = f"{resource_id}?rev={rev}"

    return resource_id


SplitEntryComponents = tuple[str, dict, dict | None]


def _iter_dataset_children(
    dataset: dict | list, branch_token: str
) -> Iterator[SplitEntryComponents]:
    if isinstance(dataset, dict):
        for key, values in dataset.items():
            yield _split_entry({"@id": key} | values, branch_token)
    elif isinstance(dataset, list):
        for entry in dataset:
            yield _split_entry(entry, branch_token)
    else:
        raise TypeError(f"Unsupported dataset type '{type(dataset)}'.")


def transform_nested_dataset(
    dataset: dict,
    level_transforms: Sequence[Callable[[str, dict], tuple[str, dict]]],
    branch_token: str = "hasPart",
) -> dict:
    """Transform a nested dataset using the level transforms.

    Args:
        dataset: The nested dataset.
        level_transforms: A list of callables that transform each nested level.
        branch_token: Token to access the children of each level. Default is 'hasPart'.

    Returns:
        Transformed dataset.
    """
    level = 0
    result: dict = {}

    if branch_token not in dataset:
        dataset = {branch_token: dataset}

    q: deque[tuple[dict, Any, int]] = deque([(dataset[branch_token], result, level)])

    while q:
        source, target, level = q.pop()

        transform_func = level_transforms[level]

        target_level = target[branch_token] = {}

        for child_id, child_data, child_children in _iter_dataset_children(source, branch_token):
            target_data = transform_func(child_id, child_data)

            target_level[utils.url_without_revision(child_id)] = target_data

            if child_children:
                q.append((child_children, target_data, level + 1))

    return result


def _split_entry(entry: dict, branch_token: str) -> SplitEntryComponents:
    """Split a level entry by separating the id, entry information, and its children data."""
    children = entry.get(branch_token, None)
    entry = {k: v for k, v in entry.items() if k != branch_token}

    if "@id" in entry:
        resource_id = entry.pop("@id")
    else:
        resource_id = entry.pop("id")

    if "_rev" in entry:
        rev = entry.pop("_rev")
    elif "rev" in entry:
        rev = entry.pop("rev")
    else:
        rev = None

    if rev is not None:
        if "?rev=" in resource_id:
            raise CWLWorkflowError(f"ID {resource_id} has already a revision.")
        resource_id = f"{resource_id}?rev={rev}"

    return resource_id, entry, children


_TRANSFORM_CACHE: dict[str, Any] = {}


def transform_cached(func):
    """Cache for transform functions."""

    def wrapped(entry_id, entry_data, **kwargs):
        if entry_id in _TRANSFORM_CACHE:
            return deepcopy(_TRANSFORM_CACHE[entry_id])

        result = func(entry_id, entry_data, **kwargs)
        _TRANSFORM_CACHE[entry_id] = deepcopy(result)
        return result

    return wrapped


@transform_cached
def get_entry_property(
    entry_id: str,
    entry_data: dict,
    *,
    property_name: str,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    """Get a level's property by key or fetch the entity and retrieve the attribute.

    Args:
        entry_id: The level's id key.
        entry_data: The level's data dictionary.
        property_name: The property to get from 'entry_data' if exists or retrieve using 'entry_id'.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        A dictionary with the property name as a key. Example: {"label": "my-label"}
    """
    try:
        value = entry_data[property_name]
    except KeyError:
        value = load_by_id(entry_id, cross_bucket=True, base=base, org=org, proj=proj, token=token)[
            property_name
        ]
    return {property_name: value}


@transform_cached
def get_distribution_path_entry(entry_id, _, *, base=None, org=None, proj=None, token=None):
    """Return distribution path entry."""
    return {
        "path": get_distribution_location_path(entry_id, base=base, org=org, proj=proj, token=token)
    }


@dataclass
class AtlasInfo:
    """Atlas paths information."""

    ontology_path: str
    annotation_path: str
    hemisphere_path: str | None
    ph_catalog: dict | None
    cell_orientation_field_path: str | None
    directory: Path

    @classmethod
    def from_file(cls, filepath: StrOrPath):
        """Instantiate from a file."""
        data = utils.load_json(filepath)
        return cls(
            ontology_path=data["ontology_path"],
            annotation_path=data["annotation_path"],
            hemisphere_path=data.get("annotation_path"),
            ph_catalog=data.get("ph_catalog"),
            cell_orientation_field_path=data.get("cell_orientation_field_path"),
            directory=Path(data["directory"]),
        )

    def to_file(self, filepath: StrOrPath):
        """Dump atlas info to a json file."""
        data = asdict(self)
        utils.write_json(data=data, filepath=filepath)


def stage_atlas(
    id_or_entity: str,
    output_dir: Path,
    *,
    parcellation_ontology_basename: str = "hierarchy.json",
    parcellation_volume_basename: str = "brain_regions.nrrd",
    parcellation_hemisphere_basename: str = "hemisphere.nrrd",
    cell_orientation_field_basename: str = "orientation.nrrd",
    symbolic=True,
    output_file: StrOrPath | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> AtlasInfo:
    """Stage an atlas to the given output_dir.

    Args:
        id_or_entity: The resource id or the entity.
        output_dir: The output directory to put the files in.
        parcellation_ontology_basename: The filename of the retrieved hierarchy.
        parcellation_volume_basename: The filename of the retrieved brain annotations
        parcellation_hemisphere_basename: The filename of the retrieved hemisphere volume.
        cell_orientation_field_basename: The filename of the cell orientation field file.
        symbolic: If True symbolic links will be attempted if the datasets exist on gpfs
            otherwise the files will be downloaded.
        output_file: Optional output file to write the atlas info file.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Return:
        An AtlasInfo instance with the staged file paths.
    """
    atlas = get_obj(id_or_entity, cls=AtlasRelease, base=base, org=org, proj=proj, token=token)

    resource_id = atlas.get_id()

    ontology_path = str(
        stage_distribution_file(
            atlas.parcellationOntology,
            output_dir=output_dir,
            encoding_format="application/json",
            filename=parcellation_ontology_basename,
            symbolic=symbolic,
            base=base,
            proj=proj,
            org=org,
            token=token,
        )
    )
    annotation_path = str(
        stage_distribution_file(
            atlas.parcellationVolume,
            output_dir=output_dir,
            encoding_format="application/nrrd",
            filename=parcellation_volume_basename,
            symbolic=symbolic,
            base=base,
            proj=proj,
            org=org,
            token=token,
        )
    )

    if atlas.hemisphereVolume:
        hemisphere_path = str(
            stage_distribution_file(
                atlas.hemisphereVolume,
                output_dir=output_dir,
                encoding_format="application/nrrd",
                filename=parcellation_hemisphere_basename,
                symbolic=symbolic,
                base=base,
                proj=proj,
                org=org,
                token=token,
            )
        )
    else:
        L.warning("Atlas Release %s has no hemisphereVolume.", resource_id)
        hemisphere_path = None

    if atlas.placementHintsDataCatalog:
        ph_catalog = materialize_ph_catalog(
            atlas.placementHintsDataCatalog,
            output_dir=output_dir,
            output_filenames={
                "placement_hints": ["[PH]1", "[PH]2", "[PH]3", "[PH]4", "[PH]5", "[PH]6"],
                "voxel_distance_to_region_bottom": "[PH]y",
            },
            symbolic=symbolic,
            base=base,
            proj=proj,
            org=org,
            token=token,
        )
    else:
        L.warning("Atlas Release %s has no placementHintsDataCatalog", resource_id)
        ph_catalog = None

    if atlas.cellOrientationField:
        cell_orientation_field_path = str(
            stage_distribution_file(
                atlas.cellOrientationField,
                output_dir=output_dir,
                encoding_format="application/nrrd",
                filename=cell_orientation_field_basename,
                symbolic=symbolic,
                base=base,
                proj=proj,
                org=org,
                token=token,
            )
        )
    else:
        L.warning("Atlas Release %s has no cellOrientationField", resource_id)
        cell_orientation_field_path = None

    atlas_info = AtlasInfo(
        ontology_path=ontology_path,
        annotation_path=annotation_path,
        hemisphere_path=hemisphere_path,
        ph_catalog=ph_catalog,
        cell_orientation_field_path=cell_orientation_field_path,
        directory=Path(output_dir),
    )

    if output_file:
        atlas_info.to_file(output_file)

    return atlas_info


def stage_file(source: Path, target: Path, symbolic: bool = True) -> None:
    """Stage a source file to the target location.

    Args:
        source: File  path to stage.
        target: Location to stage to.
        symbolic: If True a soft link will be created at target, pointing to source.
            Otherwise, a copy will be performed.

    Note: The directory structure of the target will be created if it doesn't exist.
    """
    source = Path(source)
    target = Path(target)

    if not source.exists():
        raise CWLWorkflowError(f"Path {source} does not exist.")

    if not target.parent.exists():
        target.parent.mkdir(parents=True)
        L.debug("Parent dir of %s doesn't exist. Created.", target.parent)

    target.unlink(missing_ok=True)

    if symbolic:
        os.symlink(source, target)
        L.debug("Link %s -> %s", source, target)
    else:
        shutil.copyfile(source, target)
        L.debug("Copy %s -> %s", source, target)


def _config_to_path(
    config: dict,
    *,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> str:
    """Return gpfs path of arrow file from config entry."""
    return get_distribution_location_path(
        get_entry_id(config),
        base=base,
        org=org,
        proj=proj,
        token=token,
    )


def _get_data(
    obj: dict | IdOrEntity,
    cls=Entity,
    *,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    if isinstance(obj, dict):
        return obj

    return get_distribution_as_dict(
        obj,
        cls=cls,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )


def materialize_macro_connectome_config(
    obj: dict | IdOrEntity,
    output_file: StrOrPath | None = None,
    *,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> dict:
    """Materialize the macro connectome config.

    Args:
        obj: One of the following:
            - Resource nexus id of entity to get distribution from1
            - Dictiorary of the data
            - Entity to get distribution from
        output_file: Optional path to write the result to a file. Default is None.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        The materialized dictionary of the macro connectome configuration. Example:
            {
                "initial": {"connection_strength": path/to/initial/arrow/file},
                "overrides": {"connection_strength": path/to/overrides/arrow/file}
            }

    Note: overrides key is mandatory but can be empty.
    """
    data = _get_data(obj, cls=MacroConnectomeConfig, base=base, org=org, proj=proj, token=token)

    # make it similar to micro config
    if "bases" in data:
        data["initial"] = data["bases"]
        del data["bases"]
        L.warning("Legacy 'bases' key found and was converted to 'initial'.")

    data["initial"]["connection_strength"] = _config_to_path(
        config=data["initial"]["connection_strength"],
        base=base,
        org=org,
        proj=proj,
        token=token,
    )

    if "connection_strength" in data["overrides"]:
        data["overrides"]["connection_strength"] = _config_to_path(
            config=data["overrides"]["connection_strength"],
            base=base,
            org=org,
            proj=proj,
            token=token,
        )
    else:
        L.warning("No overrides found for MacroConnectomeConfig.")

    if output_file:
        utils.write_json(filepath=output_file, data=data)

    return data


def materialize_micro_connectome_config(
    obj,
    output_file: pd.DataFrame | None = None,
    *,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> dict:
    """Materialize micro connectome config.

    Args:
        obj: One of the following:
            - Resource nexus id of entity to get distribution from1
            - Dictiorary of the data
            - Entity to get distribution from
        output_file: Optional filepath to write dictionary as json.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        Materialized dictionary of the micro connectome configuration. Example:
            {
                "variants": {
                    "var1": {
                        "params": {
                            "weight": {
                                "default": 1.0
                            }
                        }
                    }
                },
                "initial": {
                    "variants": path/to/variants/arrow/file,
                    "var1": path/to/var1-parameters/arrow/file,
                },
                "overrides": {
                    "variants": path/to/variants-overrides/arrow/file,
                    "var1": path/to/var1-parameters-overrides/arrow/file,
                },

            }

    Note: overrides key is mandatory but can be empty or include subset of keys in 'initial'.
    """

    def convert_section(section):
        """Convert a config section to pointing to gpfs paths instead of KG resources."""
        resolved_section = {}
        for entry_name, entry_data in section.items():
            # empty data, no overrides
            if not entry_data:
                continue

            if entry_name == "configuration":
                for variant_name, variant_data in entry_data.items():
                    if variant_data:
                        resolved_section[variant_name] = _config_to_path(
                            config=variant_data, base=base, org=org, proj=proj, token=token
                        )
            else:
                resolved_section[entry_name] = _config_to_path(
                    config=entry_data, base=base, org=org, proj=proj, token=token
                )

        return resolved_section

    data = _get_data(obj, cls=MicroConnectomeConfig, base=base, org=org, proj=proj, token=token)

    for section in ("initial", "overrides"):
        data[section] = convert_section(data[section])

    if output_file:
        utils.write_json(filepath=output_file, data=data)

    return data


def materialize_synapse_config(
    obj,
    output_dir,
    *,
    output_file: StrOrPath | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> dict:
    """Materialize a synapse editor config.

    Args:
        obj: One of the following:
            - Resource nexus id of entity to get distribution from1
            - Dictiorary of the data
            - Entity to get distribution from
        output_dir: Output directory to stage data.
        output_file: Optional output file to write the json config to.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        Materialized dataset dictionary.
    """
    data = _get_data(obj, cls=SynapseConfig, base=base, org=org, proj=proj, token=token)

    # backwards compatibility with placeholder empty configs
    if not data:
        return {"configuration": {}}

    result = {
        section_name: {
            dset_name: stage_distribution_file(
                get_entry_id(dset_data),
                output_dir=output_dir,
                encoding_format="application/json",
                symbolic=False,
                base=base,
                org=org,
                proj=proj,
            )
            for dset_name, dset_data in section_data.items()
        }
        for section_name, section_data in data.items()
        if section_name in {"defaults", "configuration"}
    }

    if output_file:
        utils.write_json(data=result, filepath=output_file)

    return result


def materialize_ph_catalog(
    obj,
    *,
    output_dir=None,
    output_filenames: dict | None = None,
    symbolic=True,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):  # pylint: disable=dangerous-default-value
    """Materialize placement hints catalog resources.

    Args:
        obj: One of the following:
            - Resource nexus id of entity to get distribution from1
            - Dictiorary of the data
            - Entity to get distribution from
        output_dir: Output directory to stage the files.
        output_filenames: Filenames to use for the output datasets.
        symbolic: Make symlinks where possible.
        base: The nexus base endpoint. If None entity-management's runtime base is used.
        org: The nexus organization. If None entity-management's runtime org is used.
        proj: The nexus project. If None entity-management's runtime proj is used.
        token: Optional OAuth token. If None entity-management's runtime token is used.

    Returns:
        Materialized dataset dictionary.
    """
    if output_filenames is None:
        output_filenames = {
            "placement_hints": ["[PH]1", "[PH]2", "[PH]3", "[PH]4", "[PH]5", "[PH]6"],
            "voxel_distance_to_region_bottom": "[PH]y",
        }

    def get_file_path(entry):
        return unquote_uri_path(entry["distribution"]["atLocation"]["location"])

    def materialize_path(entry, output_dir, output_filename):
        path = get_file_path(entry)

        if output_dir:
            target_path = str(Path(output_dir, output_filename + ".nrrd"))
            stage_file(source=path, target=target_path, symbolic=symbolic)
            path = target_path

        return path

    def materialize_ph(i, entry):
        path = materialize_path(entry, output_dir, output_filenames["placement_hints"][i])
        regions = {
            region: {
                "hasLeafRegionPart": region_data["hasLeafRegionPart"],
                "layer": region_data["layer"]["label"],
            }
            for region, region_data in entry["regions"].items()
        }
        return {"path": path, "regions": regions}

    data = _get_data(obj, cls=Entity, base=base, org=org, proj=proj, token=token)

    phs = [materialize_ph(i, p) for i, p in enumerate(data["placementHints"])]

    ph_y = {
        "path": materialize_path(
            entry=data["voxelDistanceToRegionBottom"],
            output_dir=output_dir,
            output_filename=output_filenames["voxel_distance_to_region_bottom"],
        ),
    }
    return {"placement_hints": phs, "voxel_distance_to_region_bottom": ph_y}


def stage_variant(
    obj,
    *,
    output_file: StrOrPath,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
):
    """Stage variant nexus resource to a cwl local definition."""
    output_file = Path(output_file)

    variant = get_obj(obj, cls=Variant, base=base, org=org, proj=proj, token=token)

    download_distribution(
        variant,
        output_dir=output_file.parent,
        filename=output_file.name,
        encoding_format="application/cwl",
        base=base,
        org=org,
        proj=proj,
        token=token,
    )

    return Variant.from_file(
        output_file,
        generator_name=variant.generator_name,
        variant_name=variant.variant_name,
        version=variant.version,
    )


def stage_detailed_circuit(
    obj,
    *,
    output_file: StrOrPath,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> None:
    """Stage DetailedCircuit a resource to its circuit config file."""
    circuit = get_obj(obj, cls=DetailedCircuit, base=base, org=org, proj=proj, token=token)
    circuit_config_path = circuit.circuitConfigPath.get_url_as_path()
    stage_file(source=Path(circuit_config_path), target=Path(output_file))

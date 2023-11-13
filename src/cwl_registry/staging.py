"""Staging utils."""
import json
import logging
import os
import shutil
from collections import deque
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Callable, Union

import pandas as pd
from entity_management.nexus import get_unquoted_uri_path
from entity_management.util import unquote_uri_path
from kgforge.core import Resource

from cwl_registry import nexus, utils
from cwl_registry.nexus import (
    get_resource,
    read_json_file_from_resource,
    read_json_file_from_resource_id,
)
from cwl_registry.validation import validate_schema

ENCODING_FORMATS = {
    "json": "application/json",
    "yaml": "application/yaml",
    "jsonld": "application/ld+json",
    "nrrd": "application/nrrd",
    None: "application/octet-stream",
    "x-neuron-hoc": "application/x-neuron-hoc",
}


L = logging.getLogger(__name__)


def _distribution_as_list(distribution: Union[Resource, list[Resource]]) -> list[Resource]:
    """Return the distribution always as a list."""
    return distribution if isinstance(distribution, list) else [distribution]


def _create_target_file(source_file: Path, output_dir: Path, basename: str | None = None):
    if basename is None:
        return output_dir / source_file.name
    return output_dir / basename


def _has_gpfs_path(distribution: Resource) -> bool:
    """Return True if the distribution has a gpfs location."""
    return hasattr(distribution, "atLocation") and distribution.atLocation.location.startswith(
        "file:///gpfs"
    )


def _remove_prefix(prefix: str, path: str) -> str:
    """Return the path without the prefix."""
    if path.startswith(prefix):
        return path[len(prefix) :]
    return path


def _find_first(predicate: Callable[[Any], bool], objects: Sequence[Any]) -> Any:
    """Return the first encounter in the object list if the predicate is satisfied."""
    for obj in objects:
        if predicate(obj):
            return obj
    return None


def _stage_distribution_with_atLocation(
    distributions: list[Resource],
    output_dir: Path,
    basename: str,
    encoding_format: str,
    symbolic: bool,
) -> Path:
    """Stage the distribution of given encoding format when atLocation is available."""
    distribution = _find_first(lambda d: d.encodingFormat == encoding_format, distributions)
    source_file = Path(unquote_uri_path(distribution.atLocation.location))
    target_file = _create_target_file(source_file, output_dir, basename)
    stage_file(source_file, target_file, symbolic=symbolic)
    return target_file


def _stage_distribution_wout_atLocation(
    forge,
    resource: Resource,
    distributions: list[Resource],
    output_dir: Path,
    basename: str,
    encoding_format: str,
):
    """Stage the distribution of given encoding when atLocation is not available."""
    forge.download(resource, "distribution.contentUrl", output_dir, cross_bucket=True)
    # cleanup all the files that we don't need, which were bundled along

    valid_distributions = filter(lambda d: isinstance(d, Resource), distributions)

    for d in valid_distributions:
        if d.encodingFormat == encoding_format:
            if basename is not None:
                target_path = output_dir / basename
                os.rename(output_dir / d.name, target_path)
            else:
                target_path = output_dir / d.name
        else:
            os.remove(output_dir / d.name)
    return target_path


def stage_resource_distribution_file(
    forge, resource_id, output_dir, encoding_type, basename=None, symbolic=True
):
    """Stage a file from a resource with given 'encoding_type'.

    Note: A resource may have many distributions with a different encoding format.
    """
    output_dir = Path(output_dir)
    resource = get_resource(forge=forge, resource_id=resource_id)
    encoding_format = ENCODING_FORMATS[encoding_type]
    distributions = _distribution_as_list(resource.distribution)
    have_gpfs_path = [_has_gpfs_path(d) for d in distributions]

    if all(have_gpfs_path):
        target_path = _stage_distribution_with_atLocation(
            distributions=distributions,
            output_dir=output_dir,
            basename=basename,
            encoding_format=encoding_format,
            symbolic=symbolic,
        )
    else:
        target_path = _stage_distribution_wout_atLocation(
            forge=forge,
            resource=resource,
            distributions=distributions,
            output_dir=output_dir,
            basename=basename,
            encoding_format=encoding_format,
        )

    assert target_path.exists()
    return target_path


def stage_me_type_densities(forge, resource_id: str, output_file: Path) -> None:
    """Stage me type densities resource."""
    resource = get_resource(forge=forge, resource_id=resource_id)

    dataset = read_json_file_from_resource(resource)

    materialize_density_distribution(forge, dataset, output_file=output_file)


def materialize_json_file_from_resource(resource, output_file: Path) -> None:
    """Materialize and optionally write a json file from a resource."""
    data = read_json_file_from_resource(resource)

    if output_file:
        utils.write_json(filepath=output_file, data=data)

    return data


def materialize_density_distribution(
    forge, dataset: dict, output_file: os.PathLike | None = None
) -> dict:
    """Materialize the me type densities distribution."""
    validate_schema(data=dataset, schema_name="cell_composition_volume_distribution.yml")

    groups = apply_to_grouped_dataset(
        forge,
        dataset,
        group_names=("mtypes", "etypes"),
        apply_function=get_distribution_path_from_resource,
    )

    if output_file:
        utils.write_json(filepath=output_file, data=groups)

    return groups


def get_distribution_path_from_resource(forge, resource_id):
    """Get json file path from resource's distribution."""
    resource = get_resource(forge=forge, resource_id=resource_id)
    # pylint: disable=protected-access
    return {
        "path": get_unquoted_uri_path(
            url=resource.distribution.contentUrl, token=forge._store.token
        )
    }


def get_entry_id(entry: dict) -> str:
    """Get entry id."""
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
        assert "?rev=" not in resource_id
        resource_id = f"{resource_id}?rev={rev}"

    return resource_id


def _iter_dataset_children(dataset, branch_token):
    if isinstance(dataset, dict):
        dataset = ({**{"@id": key}, **values} for key, values in dataset.items())

    for d in dataset:
        yield _split_entry(d, branch_token)


def transform_nested_dataset(
    dataset: dict,
    level_transforms: list[Callable[[dict], tuple[str, dict]]],
    branch_token: str | None = "hasPart",
    collapse_leaves=False,
):
    """Transform a nested dataset via the level transforms."""
    if branch_token is None:
        assert collapse_leaves is False, "Compact datasets are already collapsed."
        return _transform_compact_dataset(dataset, level_transforms)

    return _transform_expanded_dataset(dataset, level_transforms, branch_token, collapse_leaves)


def _transform_expanded_dataset(
    dataset: dict,
    level_transforms: list[Callable[[dict], tuple[str, dict]]],
    branch_token: str | None = "hasPart",
    collapse_leaves=False,
):
    level = 0
    last_level = len(level_transforms) - 1

    result = {}

    if branch_token not in dataset:
        dataset = {"hasPart": dataset}

    q = deque([(dataset["hasPart"], result, level)])

    while q:
        source, target, level = q.pop()

        transform_func = level_transforms[level]

        is_collapsible_leaf = level == last_level and collapse_leaves

        if is_collapsible_leaf:
            target_level = target
        else:
            target_level = target[branch_token] = {}

        for child_id, child_data, child_children in _iter_dataset_children(source, branch_token):
            target_data = transform_func(child_id, child_data)

            if is_collapsible_leaf:
                target_level.update(target_data)
            else:
                target_level[utils.url_without_revision(child_id)] = target_data

            if child_children:
                q.append((child_children, target_data, level + 1))

    return result


def _split_entry(entry, branch_token):
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
        assert "?rev=" not in resource_id
        resource_id = f"{resource_id}?rev={rev}"

    return resource_id, entry, children


def _transform_compact_dataset(
    dataset: dict,
    level_transforms: list[Callable[[dict], tuple[str, dict]]],
):
    level = 0
    last_level = len(level_transforms) - 1
    result = {}

    q = deque([(dataset, result, level)])

    while q:
        source, target, level = q.pop()

        transform_func = level_transforms[level]

        if level == last_level:
            source_id, source_data, _ = _split_entry(source, None)
            target_data = transform_func(source_id, source_data)
            target.update(target_data)
        else:
            target_level = target["hasPart"] = {}
            for child_id, children in source.items():
                target_data = transform_func(child_id, {})
                target_level[utils.url_without_revision(child_id)] = target_data
                q.append((children, target_data, level + 1))

    return result


_TRANSFORM_CACHE = {}


def transform_cached(func):
    """Cache for trasnform functions."""

    def wrapped(entry_id, entry_data, *args, **kwargs):
        key = entry_id

        if entry_data:
            key += json.dumps(entry_data, sort_keys=True)

        if key in _TRANSFORM_CACHE:
            return deepcopy(_TRANSFORM_CACHE[key])

        result = func(entry_id, entry_data, *args, **kwargs)

        _TRANSFORM_CACHE[key] = deepcopy(result)

        return result

    return wrapped


@transform_cached
def get_region_label(entry_id, entry_data=None, forge=None):  # pylint: disable=unused-argument
    """Get region acronym as label."""
    return {"label": nexus.get_region_resource_acronym(forge, entry_id)}


@transform_cached
def get_label(entry_id, entry_data=None, forge=None):  # pylint: disable=unused-argument
    """Get entry resource label."""
    return {"label": nexus.get_resource_json_ld(entry_id, forge)["label"]}


@transform_cached
def get_morphology_paths(entry_id, entry_data=None, forge=None):  # pylint: disable=unused-argument
    """Get morphology paths."""
    return {"path": get_distribution_path_from_resource(forge, entry_id)["path"]}


def apply_to_grouped_dataset(
    forge,
    dataset,
    group_names: list | None = None,
    apply_function=get_distribution_path_from_resource,
):
    """Materialize a grouped dataset with resource ids."""
    visited = {}

    def materialize(data, index):
        if "hasPart" in data:
            # collapse the bottom most list which always has one element
            if len(data["hasPart"]) == 1 and "hasPart" not in data["hasPart"][0]:
                return materialize(data["hasPart"][0], index + 1)

            contents = {}
            for entry in data["hasPart"]:
                res = materialize(entry, index + 1)
                contents[entry["@id"]] = {"label": _get_label(entry), **res}

            level_name = "hasPart" if group_names is None else group_names[index + 1]

            return {level_name: contents}

        resource_id = _get_id(data)

        if resource_id in visited:
            value = visited[resource_id]
        else:
            value = apply_function(forge, resource_id)
            visited[resource_id] = value

        return value

    def _get_id(entry):
        """Get id with revision if available."""
        resource_id = entry["@id"]

        if "_rev" in entry:
            assert "?rev" not in resource_id
            resource_id = f"{resource_id}?rev={entry['_rev']}"

        return resource_id

    def _get_label(entry):
        """Get label if available or fetch it from the resource."""
        if "label" not in entry:
            return get_resource(forge=forge, resource_id=entry["@id"]).label
        return entry["label"]

    return materialize(dataset, index=-1)


def stage_dataset_groups(forge, dataset_resource_id, staging_function):
    """Stage the groups in a KG dataset."""
    data = {}

    resource = get_resource(forge=forge, resource_id=dataset_resource_id)
    dataset = read_json_file_from_resource(resource)

    existing: dict[str, str] = {}

    for identifier, group in dataset.items():
        # sometimes the entries start with @context for example
        if identifier not in {"@context", "@type", "@id"}:
            entries = []
            for part in group["hasPart"]:
                entry_id = part["@id"]
                if entry_id in existing:
                    value = existing[entry_id]
                else:
                    value = staging_function(resource_id=entry_id)
                    existing[entry_id] = value

                entries.append(value)

            # TODO: Use identifiers to always get the correct label
            label = group["label"]
            data[label] = entries

    return data


def stage_mtype_morphologies(forge, resource_id: str, output_dir: Path):
    """Stage mtype morphologies."""
    staging_function = partial(
        stage_resource_distribution_file,
        forge=forge,
        output_dir=output_dir,
        encoding_type=None,
        symbolic=False,
    )
    return stage_dataset_groups(forge, resource_id, staging_function)


def stage_etype_emodels(forge, resource_id: str, output_dir: Path):
    """Stage etype models."""
    staging_function = partial(
        stage_resource_distribution_file,
        forge=forge,
        output_dir=output_dir,
        encoding_type="x-neuron-hoc",
        symbolic=False,
    )
    return stage_dataset_groups(forge, resource_id, staging_function)


@dataclass
class AtlasInfo:
    """Atlas paths information."""

    ontology_path: str
    annotation_path: str
    hemisphere_path: str | None
    ph_catalog: dict | None
    cell_orientation_field_path: str | None
    directory: Path


def stage_atlas(
    forge,
    resource_id: str,
    output_dir: Path,
    parcellation_ontology_basename: str = "hierarchy.json",
    parcellation_volume_basename: str = "brain_regions.nrrd",
    parcellation_hemisphere_basename: str = "hemisphere.nrrd",
    cell_orientation_field_basename: str = "orientation.nrrd",
    symbolic=True,
):
    """Stage an atlas to the given output_dir.

    Args:
        forge: KnowledgeGraphForge instance.
        resource_id: The resource id of the entity.
        output_dir: The output directory to put the files in.
        parcellation_ontology_basename: The filename of the retrieved hierarchy.
        parcellation_volume_basename: The filename of the retrieved brain annotations
        parcellation_hemisphere_basename: The filename of the retrieved hemisphere volume.
        symbolic: If True symbolic links will be attempted if the datasets exist on gpfs
            otherwise the files will be downloaded.
    """
    atlas = get_resource(forge=forge, resource_id=resource_id)
    assert "BrainAtlasRelease" in atlas.type

    ontology_path = str(
        stage_resource_distribution_file(
            forge,
            atlas.parcellationOntology.id,
            output_dir=output_dir,
            encoding_type="json",
            basename=parcellation_ontology_basename,
            symbolic=symbolic,
        )
    )
    annotation_path = str(
        stage_resource_distribution_file(
            forge,
            atlas.parcellationVolume.id,
            output_dir=output_dir,
            encoding_type="nrrd",
            basename=parcellation_volume_basename,
            symbolic=symbolic,
        )
    )

    try:
        hemisphere_path = str(
            stage_resource_distribution_file(
                forge,
                atlas.hemisphereVolume.id,
                output_dir=output_dir,
                encoding_type="nrrd",
                basename=parcellation_hemisphere_basename,
                symbolic=symbolic,
            )
        )
    except AttributeError:
        L.warning("Atlas Release %s has no hemisphereVolume.", resource_id)
        hemisphere_path = None

    try:
        ph_catalog = materialize_ph_catalog(
            forge,
            atlas.placementHintsDataCatalog.id,
            output_dir=output_dir,
            output_filenames={
                "placement_hints": ["[PH]1", "[PH]2", "[PH]3", "[PH]4", "[PH]5", "[PH]6"],
                "voxel_distance_to_region_bottom": "[PH]y",
            },
            symbolic=symbolic,
        )
    except AttributeError:
        L.warning("Atlas Release %s has no placementHintsDataCatalog", resource_id)
        ph_catalog = None

    try:
        cell_orientation_field_path = str(
            stage_resource_distribution_file(
                forge,
                atlas.cellOrientationField.id,
                output_dir=output_dir,
                encoding_type="nrrd",
                basename=cell_orientation_field_basename,
                symbolic=symbolic,
            )
        )
    except AttributeError:
        L.warning("Atlas Release %s has no cellOrientationField", resource_id)
        cell_orientation_field_path = None

    return AtlasInfo(
        ontology_path=ontology_path,
        annotation_path=annotation_path,
        hemisphere_path=hemisphere_path,
        ph_catalog=ph_catalog,
        cell_orientation_field_path=cell_orientation_field_path,
        directory=Path(output_dir),
    )


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

    assert source.exists(), f"Path {source} does not exist."

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


def _url_from_config(config: dict) -> str:
    """Return a url from a dictionary config entry.

    Examples:
        {"id": my-id, "rev": 2} -> my-id?rev=2
        {"id": "my-id"} -> my-id
    """
    assert "id" in config, config
    return utils.url_with_revision(
        url=config["id"],
        rev=config.get("rev", None),
    )


def _config_to_path(forge, config: dict) -> str:
    """Return gpfs path of arrow file from config entry."""
    resource_id = _url_from_config(config)
    return get_distribution_path_from_resource(forge, resource_id)["path"]


def materialize_macro_connectome_config(
    forge, resource_id: str, output_file: Path | None = None
) -> dict:
    """Materialize the macro connectome config.

    Args:
        resource_id: Id of KG resource.
        output_file: Optional path to write the result to a file. Default is None.

    Returns:
        The materialized dictionary of the macro connectome configuration. Example:
            {
                "initial": {"connection_strength": path/to/initial/arrow/file},
                "overrides": {"connection_strength": path/to/overrides/arrow/file}
            }

    Note: overrides key is mandatory but can be empty.
    """
    data = read_json_file_from_resource_id(forge, resource_id)

    # make it similar to micro config
    if "bases" in data:
        data["initial"] = data["bases"]
        del data["bases"]
        L.warning("Legacy 'bases' key found and was converted to 'initial'.")

    data["initial"]["connection_strength"] = _config_to_path(
        forge, data["initial"]["connection_strength"]
    )

    if "connection_strength" in data["overrides"]:
        data["overrides"]["connection_strength"] = _config_to_path(
            forge, data["overrides"]["connection_strength"]
        )
    else:
        L.warning("No overrides found for resource: %s", resource_id)

    if output_file:
        utils.write_json(filepath=output_file, data=data)

    return data


def materialize_micro_connectome_config(
    forge, resource_id: str, output_file: pd.DataFrame | None = None
) -> dict:
    """Materialize micro connectome config.

    Args:
        forge: KnowledgeGraphForge instance.
        resource_id: KG resource id.
        output_file: Optional output file to write the dictionary.

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
                        resolved_section[variant_name] = _config_to_path(forge, variant_data)
            else:
                resolved_section[entry_name] = _config_to_path(forge, entry_data)

        return resolved_section

    data = read_json_file_from_resource_id(forge, resource_id)

    for section in ("initial", "overrides"):
        data[section] = convert_section(data[section])

    if output_file:
        utils.write_json(filepath=output_file, data=data)

    return data


def materialize_synapse_config(forge, resource_id, output_dir):
    """Materialize a synapse editor config."""
    data = read_json_file_from_resource_id(forge, resource_id)

    # backwards compatibility with placeholder empty configs
    if not data:
        return {"configuration": {}}

    return {
        section_name: {
            dset_name: stage_resource_distribution_file(
                forge,
                get_entry_id(dset_data),
                output_dir=output_dir,
                encoding_type="json",
                symbolic=False,
            )
            for dset_name, dset_data in section_data.items()
        }
        for section_name, section_data in data.items()
    }


def materialize_ph_catalog(
    forge,
    resource_id,
    output_dir=None,
    output_filenames={
        "placement_hints": ["[PH]1", "[PH]2", "[PH]3", "[PH]4", "[PH]5", "[PH]6"],
        "voxel_distance_to_region_bottom": "[PH]y",
    },
    symbolic=True,
):  # pylint: disable=dangerous-default-value
    """Materialize placement hints catalog resources."""

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

    data = read_json_file_from_resource_id(forge, resource_id)

    phs = [materialize_ph(i, p) for i, p in enumerate(data["placementHints"])]

    ph_y = {
        "path": materialize_path(
            entry=data["voxelDistanceToRegionBottom"],
            output_dir=output_dir,
            output_filename=output_filenames["voxel_distance_to_region_bottom"],
        ),
    }
    return {"placement_hints": phs, "voxel_distance_to_region_bottom": ph_y}

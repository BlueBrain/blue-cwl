"""Staging utils."""
import logging
import os
import shutil
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from entity_management.nexus import get_file_location
from kgforge.core import Resource

from cwl_registry.nexus import get_resource, read_json_file_from_resource
from cwl_registry.utils import write_json

ENCODING_FORMATS = {
    "json": "application/json",
    "jsonld": "application/ld+json",
    "nrrd": "application/nrrd",
    None: "application/octet-stream",
    "x-neuron-hoc": "application/x-neuron-hoc",
}


L = logging.getLogger(__name__)


def _distribution_as_list(distribution: Union[Resource, List[Resource]]) -> List[Resource]:
    """Return the distribution always as a list."""
    return distribution if isinstance(distribution, list) else [distribution]


def _create_target_file(source_file: Path, output_dir: Path, basename: Optional[str] = None):
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
    distributions: List[Resource],
    output_dir: Path,
    basename: str,
    encoding_format: str,
    symbolic: bool,
) -> Path:
    """Stage the distribution of given encoding format when atLocation is available."""
    distribution = _find_first(lambda d: d.encodingFormat == encoding_format, distributions)
    source_file = Path(_remove_prefix("file://", distribution.atLocation.location))
    target_file = _create_target_file(source_file, output_dir, basename)
    stage_file(source_file, target_file, symbolic=symbolic)
    return target_file


def _stage_distribution_wout_atLocation(
    forge,
    resource: Resource,
    distributions: List[Resource],
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
        write_json(filepath=output_file, data=data)

    return data


def materialize_density_distribution(
    forge, dataset: dict, output_file: Optional[os.PathLike] = None
) -> dict:
    """Materialize the me type densities distribution."""
    groups = apply_to_grouped_dataset(
        forge,
        dataset,
        group_names=("mtypes", "etypes"),
        apply_function=get_distribution_path_from_resource,
    )

    if output_file:
        write_json(filepath=output_file, data=groups)

    return groups


def materialize_connectome_dataset(forge, dataset: dict, output_file: Optional[os.PathLike] = None):
    """Materialize a connectome dataset."""
    visited = {}

    def get_label(resource_id):
        """Get label if available or fetch it from the resource."""
        if resource_id in visited:
            return visited[resource_id]
        label = get_resource(forge, resource_id).label
        visited[resource_id] = label
        return label

    def get_region_notation(resource_id):
        if resource_id in visited:
            return visited[resource_id]
        label = get_resource(forge, resource_id).notation
        visited[resource_id] = label
        return label

    hemispheres = ("undefined", "left", "right")

    rows = []

    input_names = None

    # pylint: disable=too-many-nested-blocks
    for hi, hid in dataset["hasPart"].items():
        hi_label = hemispheres[int(hi)]
        for hj, hjd in hid["hasPart"].items():
            hj_label = hemispheres[int(hj)]
            for ri, rid in hjd["hasPart"].items():
                ri_label = get_region_notation(ri)
                for rj, rjd in rid["hasPart"].items():
                    rj_label = get_region_notation(rj)
                    for mi, mid in rjd["hasPart"].items():
                        mi_label = get_label(mi)
                        for mj, mjd in mid["hasPart"].items():
                            mj_label = get_label(mj)

                            if input_names is None:
                                input_names = [inp["name"] for inp in mjd["hasPart"]["inputs"]]

                            row = [hi_label, hj_label, ri_label, rj_label, mi_label, mj_label] + [
                                inp["value"] for inp in mjd["hasPart"]["inputs"]
                            ]
                            rows.append(row)

    hierarchy_names = ["hi", "hj", "ri", "rj", "mi", "mj"]

    df = pd.DataFrame(rows, columns=hierarchy_names + input_names)

    if output_file:
        df.to_json(output_file, orient="records")

    return df


def get_distribution_path_from_resource(forge, resource_id):
    """Get json file path from resource's distribution."""
    resource = get_resource(forge=forge, resource_id=resource_id)
    return {"path": get_file_location(resource.distribution.contentUrl).removeprefix("file://")}


def apply_to_grouped_dataset(
    forge,
    dataset,
    group_names: Optional[list] = None,
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
                contents[entry["@id"]] = {"label": get_label(entry), **res}

            level_name = "hasPart" if group_names is None else group_names[index + 1]

            return {level_name: contents}

        resource_id = get_id(data)

        if resource_id in visited:
            value = visited[resource_id]
        else:
            value = apply_function(forge, resource_id)
            visited[resource_id] = value

        return value

    def get_id(entry):
        """Get id with revision if available."""
        resource_id = entry["@id"]

        if "_rev" in entry:
            assert "?rev" not in resource_id
            resource_id = f"{resource_id}?rev={entry['_rev']}"

        return resource_id

    def get_label(entry):
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

    existing: Dict[str, str] = {}

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


def stage_atlas(
    forge,
    resource_id: str,
    output_dir: Path,
    parcellation_ontology_basename: Optional[str] = "hierarchy.json",
    parcellation_volume_basename: Optional[str] = "brain_regions.nrrd",
    symbolic=True,
):
    """Stage an atlas to the given output_dir.

    Args:
        forge: KnowledgeGraphForge instance.
        resource_id: The resource id of the entity.
        output_dir: The output directory to put the files in.
        parcellation_ontology_basename: The filename of the retrieved hierarchy.
        parcellation_volume_basename: The filename of the retrieved annotation.
        symbolic: If True symbolic links will be attempted if the datasets exist on gpfs
            otherwise the files will be downloaded.
    """
    atlas = get_resource(forge=forge, resource_id=resource_id)
    assert "BrainAtlasRelease" in atlas.type

    ontology_path = stage_resource_distribution_file(
        forge,
        atlas.parcellationOntology.id,
        output_dir=output_dir,
        encoding_type="json",
        basename=parcellation_ontology_basename,
        symbolic=symbolic,
    )
    volume_path = stage_resource_distribution_file(
        forge,
        atlas.parcellationVolume.id,
        output_dir=output_dir,
        encoding_type="nrrd",
        basename=parcellation_volume_basename,
        symbolic=symbolic,
    )
    return ontology_path, volume_path


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

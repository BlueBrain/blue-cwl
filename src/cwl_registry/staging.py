"""Staging utils."""
import logging
import os
import shutil
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

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


def stage_me_type_densities(forge, resource_id: str, output_file: Path):
    """Stage me type densities resource."""
    resource = get_resource(forge=forge, resource_id=resource_id)

    dataset = read_json_file_from_resource(resource)

    mtype_groups = {}

    for mtype_part in dataset["hasPart"]:
        etype_groups = {}

        for etype_part in mtype_part["hasPart"]:
            me_resource = get_resource(forge=forge, resource_id=etype_part["hasPart"][0]["@id"])
            etype_groups[etype_part["@id"]] = {
                "label": etype_part["label"],
                "path": _remove_prefix(
                    "file://", get_file_location(me_resource.distribution.contentUrl)
                ),
            }

        mtype_groups[mtype_part["@id"]] = {
            "label": mtype_part["label"],
            "etypes": etype_groups,
        }
    write_json(filepath=output_file, data={"mtypes": mtype_groups})


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

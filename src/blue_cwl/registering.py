# SPDX-License-Identifier: Apache-2.0

"""Registering utilities."""

import logging
from pathlib import Path

from entity_management import nexus
from entity_management.atlas import (
    AtlasRelease,
    CellComposition,
    CellCompositionSummary,
    CellCompositionVolume,
    METypeDensity,
)
from entity_management.base import BrainLocation, Derivation, OntologyTerm
from entity_management.core import DataDownload, Subject
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity

from blue_cwl.typing import StrOrPath
from blue_cwl.utils import load_json, write_json

L = logging.getLogger()


def _subject(species_id: str | None) -> Subject:
    if not species_id:
        species_id = "http://purl.obolibrary.org/obo/NCBITaxon_10090"
        label = "Mus musculus"
    else:
        label = nexus.load_by_id(species_id, cross_bucket=True)["label"]
    return Subject(species=OntologyTerm(url=species_id, label=label))


def _brain_location(brain_region_id: str) -> BrainLocation:
    label = nexus.load_by_id(brain_region_id, cross_bucket=True)["label"]
    return BrainLocation(brainRegion=OntologyTerm(url=brain_region_id, label=label))


def register_partial_circuit(
    name: str,
    brain_region_id: str,
    atlas_release_id: str,
    sonata_config_path: StrOrPath,
    description: str = "",
    species_id: str | None = None,
) -> DetailedCircuit:
    """Register a partial circuit."""
    atlas_release = get_entity(resource_id=atlas_release_id, cls=AtlasRelease)

    circuit_config_path = DataDownload(url=f"file://{Path(sonata_config_path).resolve()}")

    return DetailedCircuit(
        name=name,
        subject=_subject(species_id),
        description=description,
        brainLocation=_brain_location(brain_region_id),
        atlasRelease=atlas_release,
        circuitConfigPath=circuit_config_path,
    ).publish()


def register_cell_composition_summary(
    name: str,
    distribution_file: StrOrPath,
    atlas_release,
    derivation_entity,
    *,
    base=None,
    org=None,
    proj=None,
    token=None,
) -> CellCompositionSummary:
    """Create and register a cell composition summary."""
    distribution = DataDownload.from_file(
        file_like=str(distribution_file),
        content_type="application/json",
        base=base,
        org=org,
        proj=proj,
        use_auth=token,
    )
    derivation = Derivation(entity=derivation_entity)
    summary = CellCompositionSummary(
        name=name,
        about=["nsg:Neuron", "nsg:Glia"],
        description="Statistical summary of the model cell composition.",
        atlasRelease=atlas_release,
        brainLocation=atlas_release.brainLocation,
        distribution=distribution,
        derivation=derivation,
        subject=atlas_release.subject,
    )
    return summary.publish(base=base, org=org, proj=proj, use_auth=token)


def register_cell_composition_volume(
    name: str,
    distribution_file: StrOrPath,
    atlas_release,
    derivation_entity,
    *,
    base=None,
    org=None,
    proj=None,
    token=None,
) -> CellCompositionSummary:
    """Create and register a cell composition summary."""
    distribution = DataDownload.from_file(
        file_like=str(distribution_file),
        content_type="application/json",
        base=base,
        org=org,
        proj=proj,
        use_auth=token,
    )
    derivation = Derivation(entity=derivation_entity)
    volume = CellCompositionVolume(
        name=name,
        about=["nsg:Neuron", "nsg:Glia"],
        description="NRRD volume distribution of the cell composition.",
        atlasRelease=atlas_release,
        brainLocation=atlas_release.brainLocation,
        distribution=distribution,
        derivation=derivation,
        subject=atlas_release.subject,
    )
    return volume.publish(base=base, org=org, proj=proj, use_auth=token)


def register_densities(atlas_release, cell_composition_volume_file, output_file=None):
    """Register METypeDensity volumes."""
    volumes_dict = load_json(cell_composition_volume_file)

    derivation = Derivation(entity=atlas_release)
    subject = atlas_release.subject
    brain_location = atlas_release.brainLocation

    for mtype_data in volumes_dict["hasPart"]:
        for etype_data in mtype_data["hasPart"]:
            for nrrd_data in etype_data["hasPart"]:
                if nrrd_file := nrrd_data.pop("path", None):
                    me_density = _register_me_density(
                        distribution_file=nrrd_file,
                        atlas_release=atlas_release,
                        brain_location=brain_location,
                        derivation=derivation,
                        subject=subject,
                    )

                    nrrd_data["@id"] = me_density.get_id()
                    nrrd_data["_rev"] = me_density.get_rev()

                    L.debug(
                        "Registered METypeDensity file %s registered as %s",
                        nrrd_file,
                        me_density.get_id(),
                    )

    if output_file is not None:
        write_json(data=volumes_dict, filepath=output_file)


def _register_me_density(
    distribution_file: StrOrPath,
    atlas_release,
    brain_location,
    derivation,
    subject,
    *,
    base=None,
    org=None,
    proj=None,
    token=None,
):
    """Register an METypeDensity."""
    distribution = DataDownload.from_file(
        file_like=str(distribution_file),
        content_type="application/nrrd",
        base=base,
        org=org,
        proj=proj,
        use_auth=token,
    )
    entity = METypeDensity(
        name=distribution.name,  # pylint: disable=no-member
        atlasRelease=atlas_release,
        distribution=distribution,
        derivation=derivation,
        brainLocation=brain_location,
        subject=subject,
    )
    return entity.publish(base=base, org=org, proj=proj, use_auth=token)


def register_cell_composition(
    atlas_release,
    cell_composition_volume_file: StrOrPath,
    cell_composition_summary_file: StrOrPath,
    *,
    base=None,
    org=None,
    proj=None,
    token=None,
):
    """Register CellComposition."""
    summary = register_cell_composition_summary(
        name="Cell Composition Summary",
        distribution_file=cell_composition_summary_file,
        atlas_release=atlas_release,
        derivation_entity=atlas_release,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    volume = register_cell_composition_volume(
        name="Cell Composition Volume",
        distribution_file=cell_composition_volume_file,
        atlas_release=atlas_release,
        derivation_entity=atlas_release,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    cell_composition = CellComposition(
        name="Cell Composition",
        about=["nsg:Neuron", "nsg:Glia"],
        atlasRelease=atlas_release,
        atlasSpatialReferenceSystem=atlas_release.spatialReferenceSystem,
        brainLocation=atlas_release.brainLocation,
        cellCompositionVolume=volume,
        cellCompositionSummary=summary,
    )
    return cell_composition.publish(base=base, org=org, proj=proj, use_auth=token, include_rev=True)

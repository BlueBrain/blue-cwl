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
from entity_management.core import Agent, Contribution, DataDownload, Entity, Subject
from entity_management.simulation import DetailedCircuit
from entity_management.state import get_user_id
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


def _get_contribution(
    base=None,
    org=None,
    proj=None,
    token=None,
):
    return [
        Contribution(
            agent=get_entity(
                resource_id=get_user_id(base=base, org=org),
                cls=Agent,
                base=base,
                org=org,
                proj=proj,
                token=token,
            ),
        )
    ]


def register_partial_circuit(
    *,
    name: str,
    brain_region_id: str,
    atlas_release: AtlasRelease,
    sonata_config_path: StrOrPath,
    description: str = "",
    species_id: str | None = None,
    contribution: Contribution | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> DetailedCircuit:
    """Register a partial circuit."""
    contribution = contribution or _get_contribution(base=base, org=org, proj=proj, token=token)

    circuit_config_path = DataDownload(url=f"file://{Path(sonata_config_path).resolve()}")

    return DetailedCircuit(
        name=name,
        subject=_subject(species_id),
        description=description,
        brainLocation=_brain_location(brain_region_id),
        atlasRelease=atlas_release,
        circuitConfigPath=circuit_config_path,
        contribution=contribution,
    ).publish(include_rev=True, base=base, org=org, proj=proj, use_auth=token)


def register_cell_composition_summary(
    *,
    name: str,
    description: str,
    distribution_file: StrOrPath,
    atlas_release: AtlasRelease,
    derivation_entity: Entity,
    contribution: Entity | None = None,
    base=None,
    org=None,
    proj=None,
    token=None,
) -> CellCompositionSummary:
    """Create and register a cell composition summary."""
    contribution = contribution or _get_contribution(base=base, org=org, proj=proj, token=token)

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
        description=description,
        about=["nsg:Neuron", "nsg:Glia"],
        atlasRelease=atlas_release,
        brainLocation=atlas_release.brainLocation,
        distribution=distribution,
        derivation=derivation,
        contribution=contribution,
        subject=atlas_release.subject,
    )
    return summary.publish(base=base, org=org, proj=proj, use_auth=token)


def register_cell_composition_volume(
    *,
    name: str,
    description: str,
    distribution_file: StrOrPath,
    atlas_release: AtlasRelease,
    derivation_entity: Entity,
    contribution: Contribution | None = None,
    base=None,
    org=None,
    proj=None,
    token=None,
) -> CellCompositionSummary:
    """Create and register a cell composition summary."""
    contribution = contribution or _get_contribution(base=base, org=org, proj=proj, token=token)

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
        description=description,
        about=["nsg:Neuron", "nsg:Glia"],
        atlasRelease=atlas_release,
        brainLocation=atlas_release.brainLocation,
        distribution=distribution,
        derivation=derivation,
        contribution=contribution,
        subject=atlas_release.subject,
    )
    return volume.publish(base=base, org=org, proj=proj, use_auth=token)


def register_densities(
    *,
    atlas_release: AtlasRelease,
    distribution_file: StrOrPath,
    output_file: StrOrPath | None = None,
    contribution: Contribution | None = None,
    base: str | None = None,
    org: str | None = None,
    proj: str | None = None,
    token: str | None = None,
) -> dict:
    """Register METypeDensity volumes."""
    contribution = contribution or _get_contribution(base=base, org=org, proj=proj, token=token)

    volumes_dict = load_json(distribution_file)

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
                        contribution=contribution,
                        base=base,
                        org=org,
                        proj=proj,
                        token=token,
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

    return volumes_dict


def _register_me_density(
    *,
    distribution_file: StrOrPath,
    atlas_release: AtlasRelease,
    brain_location: BrainLocation,
    derivation: Derivation,
    contribution: Contribution,
    subject: Subject,
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
        contribution=contribution,
    )
    return entity.publish(base=base, org=org, proj=proj, use_auth=token)


def register_cell_composition(
    *,
    name: str,
    description: str,
    atlas_release: AtlasRelease,
    cell_composition_volume_file: StrOrPath,
    cell_composition_summary_file: StrOrPath,
    contribution: Contribution | None = None,
    base=None,
    org=None,
    proj=None,
    token=None,
):
    """Register CellComposition."""
    contribution = contribution or _get_contribution(base=base, org=org, proj=proj, token=token)

    summary = register_cell_composition_summary(
        name="Cell Composition Summary",
        description="Cell Composition Summary Distribution",
        distribution_file=cell_composition_summary_file,
        atlas_release=atlas_release,
        derivation_entity=atlas_release,
        contribution=contribution,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    volume = register_cell_composition_volume(
        name="Cell Composition Volume",
        description="Cell Composition Volume Distribution",
        distribution_file=cell_composition_volume_file,
        atlas_release=atlas_release,
        derivation_entity=atlas_release,
        contribution=contribution,
        base=base,
        org=org,
        proj=proj,
        token=token,
    )
    cell_composition = CellComposition(
        name=name,
        description=description,
        about=["nsg:Neuron", "nsg:Glia"],
        atlasRelease=atlas_release,
        atlasSpatialReferenceSystem=atlas_release.spatialReferenceSystem,
        brainLocation=atlas_release.brainLocation,
        cellCompositionVolume=volume,
        cellCompositionSummary=summary,
        contribution=contribution,
    )
    return cell_composition.publish(base=base, org=org, proj=proj, use_auth=token, include_rev=True)

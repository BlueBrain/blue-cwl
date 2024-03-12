"""Registering utilities."""

import os
from pathlib import Path

from entity_management import nexus
from entity_management.atlas import AtlasRelease, CellCompositionSummary
from entity_management.base import BrainLocation, Derivation, Identifiable, OntologyTerm
from entity_management.core import DataDownload, Subject
from entity_management.simulation import DetailedCircuit
from entity_management.util import get_entity


def _subject(species_id: str | None):
    if not species_id:
        species_id = "http://purl.obolibrary.org/obo/NCBITaxon_10090"
        label = "Mus musculus"
    else:
        label = nexus.load_by_id(species_id, cross_bucket=True)["label"]
    return Subject(species=OntologyTerm(url=species_id, label=label))


def _brain_location(brain_region_id):
    label = nexus.load_by_id(brain_region_id, cross_bucket=True)["label"]
    return BrainLocation(
        brainRegion=OntologyTerm(
            url=brain_region_id,
            label=label,
        )
    )


def register_partial_circuit(
    name,
    brain_region_id,
    atlas_release_id,
    sonata_config_path,
    description="",
    species_id=None,
):
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
    summary_file: os.PathLike,
    atlas_release_id: str,
    derivation_entity_id: str,
    *,
    base=None,
    org=None,
    proj=None,
    token=None,
):
    """Create and register a cell composition summary."""
    atlas_release = get_entity(resource_id=atlas_release_id, cls=AtlasRelease)

    distribution = DataDownload.from_file(
        file_like=str(summary_file),
        content_type="application/json",
        base=base,
        org=org,
        proj=proj,
        use_auth=token,
    )
    derivation = Derivation(
        entity=get_entity(
            resource_id=derivation_entity_id,
            cls=Identifiable,
            base=base,
            org=org,
            proj=proj,
            token=token,
        ),
    )
    summary = CellCompositionSummary(
        name=name,
        about=["nsg:Neuron", "nsg:Glia"],
        description="Statistical summary of the model cell composition.",
        atlasRelease=atlas_release,
        distribution=distribution,
        derivation=derivation,
    )
    return summary.publish(base=base, org=org, proj=proj, use_auth=token)

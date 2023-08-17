"""Registering utilities."""
import os
from pathlib import Path

from entity_management.atlas import AtlasBrainRegion, AtlasRelease
from entity_management.base import BrainLocation, OntologyTerm
from entity_management.core import DataDownload, Subject
from entity_management.simulation import DetailedCircuit
from kgforge.core import Resource


def _subject(forge, species_id):
    if species_id:
        species = _as_reference(forge, species_id, properties=["id", "label"])
    else:
        species = Resource(
            id="http://purl.obolibrary.org/obo/NCBITaxon_10090", label="Mus musculus"
        )
    resource = Resource(type="Subject", species=species)
    forge.register(resource)
    return forge.reshape(resource, ["id", "type", "species"])


def _subject_2(species_id: str | None):
    if not species_id:
        species_id = "http://purl.obolibrary.org/obo/NCBITaxon_10090"

    subject = Subject(species=OntologyTerm(url=species_id))
    subject.publish()
    return subject


def _circuit_config_path(path):
    return Resource.from_json(
        {
            "type": "DataDownload",
            "url": "file://" + str(Path(path).resolve()),
        }
    )


def _circuit_config_path_2(path):
    path = Path(path).resolve()
    return DataDownload(url=f"file://{path}")


def _brain_location(brain_region_id):
    region = AtlasBrainRegion.from_id(brain_region_id)
    return BrainLocation(
        brainRegion=OntologyTerm(
            url=region.get_id(),
            label=region.label,
        )
    )


def _as_reference(forge, entity_id, properties=("id", "type")):
    entity = forge.retrieve(entity_id, cross_bucket=True)
    return forge.reshape(entity, properties)


def register_partial_circuit(
    name,
    brain_region_id,
    atlas_release_id,
    sonata_config_path,
    description="",
    species_id=None,
):
    """Register a partial circuit."""
    circuit = DetailedCircuit(
        name=name,
        subject=_subject_2(species_id),
        description=description,
        brainLocation=_brain_location(brain_region_id),
        atlasRelease=AtlasRelease.from_id(atlas_release_id),
        circuitConfigPath=_circuit_config_path_2(sonata_config_path),
    )
    circuit.publish()
    return circuit


def _add_workflow_influence(resource):
    """Add influence from bbp-workflow execution if any."""
    if "NEXUS_WORKFLOW" in os.environ:
        resource.wasInfluencedBy = Resource(
            id=os.environ["NEXUS_WORKFLOW"],
            type="WorkflowExecution",
        )


def _as_derivation(forge, entity_id, properties=("id", "type")):
    return Resource(
        type="Derivation",
        entity=_as_reference(forge, entity_id, properties=properties),
    )


def register_cell_composition_summary(
    forge, name, summary_file, atlas_release_id, derivation_entity_id
):
    """Create and register a cell composition summary."""
    summary = Resource.from_json(
        {
            "name": name,
            "description": "Statistical summary of the model cell composition.",
            "type": ["CellCompositionSummary", "Entity"],
            "about": ["nsg:Neuron", "nsg:Glia"],
            "atlasRelease": _as_reference(forge, atlas_release_id),
            "distribution": forge.attach(str(summary_file), content_type="application/json"),
            "derivation": _as_derivation(forge, derivation_entity_id),
        }
    )

    _add_workflow_influence(summary)

    forge.register(summary)
    return summary

"""Registering utilities."""
from pathlib import Path

from kgforge.core import Resource


def _subject(forge, species_id):

    if species_id:
        species = _as_reference(forge, species_id, properties=["id", "label"])
    else:
        species = Resource(
            id="http://purl.obolibrary.org/obo/NCBITaxon_10090",
            label=["Mus musculus", "Mus Musculus"],
        )
    return Resource(
        type="Subject",
        species=species,
    )


def _circuit_config_path(path):
    return Resource.from_json(
        {
            "type": "DataDownload",
            "url": "file://" + str(Path(path).resolve()),
        }
    )


def _brain_location(forge, brain_region_id):
    return Resource(
        brainRegion=_as_reference(forge, brain_region_id, properties=["id", "label", "notation"]),
        type="BrainLocation",
    )


def _as_reference(forge, entity_id, properties=("id", "type")):
    entity = forge.retrieve(entity_id, cross_bucket=True)
    return forge.reshape(entity, properties)


def register_partial_circuit(
    forge,
    name,
    brain_region_id,
    atlas_release_id,
    sonata_config_path,
    target_digest,
    description="",
    species_id=None,
):
    """Register a partial circuit."""
    was_generated_by = Resource(
        type="BMOTask",
        targetDigest=target_digest,
    )

    circuit = Resource(
        type="DetailedCircuit",
        name=name,
        subject=_subject(forge, species_id),
        description=description,
        brainLocation=_brain_location(forge, brain_region_id),
        atlasRelease=_as_reference(forge, atlas_release_id),
        circuitConfigPath=_circuit_config_path(sonata_config_path),
        wasGeneratedBy=was_generated_by,
    )
    forge.register(circuit)

    return circuit


def _as_derivation(forge, entity_id, properties=("id", "type")):
    return Resource(
        type="Derivation",
        entity=_as_reference(forge, entity_id, properties=properties),
    )


def register_cell_composition_summary(
    forge, name, summary_file, atlas_release_id, derivation_entity_id, target_digest=None
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

    if target_digest:
        summary.wasGeneratedBy = Resource(
            type="BMOTask",
            targetDigest=target_digest,
        )

    forge.register(summary)
    return summary

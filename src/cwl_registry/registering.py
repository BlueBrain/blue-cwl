"""Registering utilities."""
from pathlib import Path

from kgforge.core import Resource


def _mouse_species():
    return Resource.from_json(
        {
            "id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
            "type": "Class",
            "label": ["Mus musculus", "Mus Musculus"],
            "subClassOf": ["nsg:Species", "obo:NCBITaxon_10088"],
        }
    )


def _isocortex_region():
    return Resource.from_json(
        {
            "id": "http://api.brain-map.org/api/v2/data/Structure/315",
            "type": "Class",
            "label": "Isocortex",
            "isDefinedBy": "http://bbp.epfl.ch/neurosciencegraph/ontologies/core/brainregion",
            "notation": "Isocortex",
            "prefLabel": "Isocortex",
            "subClassOf": "nsg:BrainRegion",
        }
    )


def register_partial_circuit(
    forge, name, brain_region, sonata_config_path, target_digest, description="", species=None
):
    """Register a partial circuit."""
    was_generated_by = Resource(
        type="BMOTask",
        targetDigest=target_digest,
    )

    brain_location = Resource(brainRegion=Resource(id=brain_region), type="BrainLocation")

    circuit = Resource(
        type="DetailedCircuit",
        name=name,
        species=Resource(id=species) if species else _mouse_species(),
        description=description,
        brainLocation=brain_location,
        circuitConfigPath="file://" + str(Path(sonata_config_path).resolve()),
        wasGeneratedBy=was_generated_by,
    )

    forge.register(circuit)

    return circuit

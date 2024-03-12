"""Topological synthesis inputs overrides."""

from copy import deepcopy

import numpy as np

from cwl_registry.mmodel.schemas import SynthesisOverrides


def apply_overrides(
    parameters: dict, distributions: dict, overrides: SynthesisOverrides
) -> tuple[dict, dict]:
    """Apply synthesis inputs overrides."""
    parameters = deepcopy(parameters)
    distributions = deepcopy(distributions)

    available_grow_types = parameters["grow_types"]

    for grow_type, neurite_overrides in overrides.items():
        assert grow_type in available_grow_types

        neurite_parameters = parameters[grow_type]
        neurite_distributions = distributions[grow_type]

        if neurite_overrides.total_extent:
            neurite_distributions["persistence_diagram"] = _scale_barcode_list(
                neurite_distributions["persistence_diagram"],
                neurite_overrides.total_extent,
            )

        if neurite_overrides.randomness:
            neurite_parameters["randomness"] = neurite_overrides.randomness

        if neurite_overrides.radius:
            neurite_parameters["radius"] = neurite_overrides.radius

        if neurite_overrides.step_size:
            neurite_parameters["step_size"] = neurite_overrides.step_size

        if neurite_overrides.orientation:
            neurite_parameters["orientation"] = {
                "mode": "normal_pia_constraint",
                "values": {
                    "direction": {
                        "mean": _angle_between(neurite_overrides.orientation[0], [0.0, 1.0, 0.0]),
                        "std": 0.0,
                    }
                },
            }

    return parameters, distributions


def _angle_between(v1, v2):
    v1 = np.array(v1)
    v2 = np.array(v2)
    v1 /= np.linalg.norm(v1)
    v2 /= np.linalg.norm(v2)
    return float(np.arccos(np.clip(np.dot(v1, v2), -1.0, 1.0)))


def _scale_barcode_list(barcode_list: list, total_extent: float) -> list:
    def scale_spatial_barcode_component(barcode: list, total_extent: float) -> list:
        barcode_array = np.array(barcode)
        barcode_array[:, (0, 1)] *= total_extent / np.nanmax(barcode_array[:, (0, 1)])
        return barcode_array.tolist()

    return [scale_spatial_barcode_component(barcode, total_extent) for barcode in barcode_list]

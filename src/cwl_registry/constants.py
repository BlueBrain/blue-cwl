"""Workflow cosntants."""

DEFAULT_CIRCUIT_BUILD_PARAMETERS = {
    "place_cells": {
        "soma_placement": "basic",
        "density_factor": 1.0,
        "sort_by": ["region", "mtype"],
        "seed": 0,
        "mini_frequencies": False,
    },
    "assign_morphologies": {
        "max_drop_ratio": 0.1,
        "seed": 0,
    },
    "choose_morphologies": {
        "alpha": 3.0,
        "seed": 0,
    },
    "synthesize_morphologies": {
        "max_drop_ratio": 0.1,
        "max_files_per_dir": 1024,
        "seed": 0,
        "scaling_jitter_std": 0.2,
        "rotational_jitter_std": 10.0,
    },
    "assign_emodels": {"seed": 0},
}

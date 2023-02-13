#!/bin/bash

set -e

# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/322?rev=16"

ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/8586fff5-8212-424c-bb52-73b514e93422?rev=1"

CELL_COMPOSITION="https://bbp.epfl.ch/neurosciencegraph/data/c5c3f334-0ba4-4149-b1dd-0abc799cf23a?rev=1"

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/0f3562ba-c393-4b7b-96c6-7d1a4259d8a7?rev=1"


export NEXUS_BASE="https://staging.nise.bbp.epfl.ch/nexus/v1"
export NEXUS_ORG="bbp_test"
export NEXUS_PROJ="studio_data_11"


cwl-registry -vv execute neurons-me-type-property \
    --region $REGION \
    --atlas $ATLAS \
    --variant-config $VARIANT_CONFIG \
    --me-type-densities $CELL_COMPOSITION \
    --output-dir ./out

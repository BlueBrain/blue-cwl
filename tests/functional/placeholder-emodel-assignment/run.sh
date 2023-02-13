#!/bin/bash

set -e

# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/322?rev=16"

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/31cdc430-fd03-4669-844c-f398f5a72f5e?rev=1"
# URL: https://staging.nise.bbp.epfl.ch/nexus/v1/resources/bbp_test/studio_data_11/_/31cdc430-fd03-4669-844c-f398f5a72f5e?rev=1

CIRCUIT="https://bbp.epfl.ch/neurosciencegraph/data/8aef7c10-6cf3-44ee-8e51-ddedb4f3dab5?rev=1"
# URL: https://staging.nise.bbp.epfl.ch/nexus/v1/resources/bbp_test/studio_data_11/_/8aef7c10-6cf3-44ee-8e51-ddedb4f3dab5?rev=1

# Production
# ETYPE_MODELS="https://bbp.epfl.ch/neurosciencegraph/data/000ccb05-8518-47ff-b726-87ff3975e2da?rev=1"

# Staging
ETYPE_MODELS="https://bbp.epfl.ch/neurosciencegraph/data/8fb6c394-b544-47fa-9f29-17defbdff09c?rev=1"
# URL: https://staging.nise.bbp.epfl.ch/nexus/v1/resources/bbp_test/studio_data_11/_/8fb6c394-b544-47fa-9f29-17defbdff09c?rev=1


cwl-registry -vv execute placeholder-emodel-assignment \
    --region $REGION \
    --partial-circuit $CIRCUIT \
    --etype-emodels $ETYPE_MODELS \
    --variant-config $VARIANT_CONFIG \
    --output-dir ./out

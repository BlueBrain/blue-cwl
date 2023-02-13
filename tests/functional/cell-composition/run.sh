#!/bin/bash

set -e

# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/322?rev=16"

STAGING=true

if [ "$STAGING" = true ] ; then

ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/8586fff5-8212-424c-bb52-73b514e93422?rev=1"
RECIPE="https://bbp.epfl.ch/neurosciencegraph/data/99f0f32c-5757-4dfa-af70-539e079972bb?rev=3"
DENSITY_DISTRIBUTION="https://bbp.epfl.ch/neurosciencegraph/data/1f678feb-6d4b-4917-83a0-8966b27a1dc2?rev=2"
BASE_COMPOSITION_SUMMARY="https://bbp.epfl.ch/neurosciencegraph/data/f5094a3a-5dff-45d7-9b04-5db1a162e01a"
VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/a5c5d83c-4f02-455d-87c1-17f75401d7d7?rev=1"

else

export NEXUS_BASE="https://bbp.epfl.ch/nexus/v1"
export NEXUS_ORG="bbp"
export NEXUS_PROJ="mmb-point-neuron-framework-model"

# Primary somatosensory area (SSp)
ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885?rev=2"
DENSITY_DISTRIBUTION="https://bbp.epfl.ch/neurosciencegraph/data/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1"
BASE_COMPOSITION_SUMMARY="https://bbp.epfl.ch/neurosciencegraph/data/f5094a3a-5dff-45d7-9b04-5db1a162e01a?rev=1"
RECIPE="https://bbp.epfl.ch/neurosciencegraph/data/1743b844-5206-4a53-9890-76ac492d5c6b?rev=1"
VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/90c9c651-706d-4ecb-bf3a-7eabfe5f2f89?rev=1"

fi

cwl-registry -vv execute cell-composition-manipulation \
    --region $REGION \
    --variant-config $VARIANT_CONFIG \
    --base-composition-summary $BASE_COMPOSITION_SUMMARY\
    --base-density-distribution $DENSITY_DISTRIBUTION \
    --atlas-release $ATLAS \
    --recipe $RECIPE \
    --output-dir ./out

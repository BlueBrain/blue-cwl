# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/322?rev=16"

# prod
ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885?rev=2"

# prod
# https://bbp.epfl.ch/nexus/v1/resources/bbp/mmb-point-neuron-framework-model/_/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1
DENSITY_DISTRIBUTION="https://bbp.epfl.ch/neurosciencegraph/data/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1"

# prod
BASE_COMPOSITION_SUMMARY="https://bbp.epfl.ch/neurosciencegraph/data/f5094a3a-5dff-45d7-9b04-5db1a162e01a?rev=1"

# prod
RECIPE="https://bbp.epfl.ch/neurosciencegraph/data/1743b844-5206-4a53-9890-76ac492d5c6b?rev=1"
VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/90c9c651-706d-4ecb-bf3a-7eabfe5f2f89?rev=1"

# staging
# RECIPE="https://bbp.epfl.ch/neurosciencegraph/data/99f0f32c-5757-4dfa-af70-539e079972bb?rev=1"


#rm -rf out && mkdir out

cwl-registry -vv execute cell-composition-manipulation \
    --region $REGION \
    --nexus-base ${NEXUS_BASE:-"https://bbp.epfl.ch/nexus/v1"} \
    --nexus-org ${NEXUS_ORG:-"bbp"} \
    --nexus-project ${NEXUS_PROJ:-"mmb-point-neuron-framework-model"} \
    --nexus-token $NEXUS_TOKEN \
    --base-composition-summary $BASE_COMPOSITION_SUMMARY\
    --base-density-distribution $DENSITY_DISTRIBUTION \
    --atlas-release $ATLAS \
    --recipe $RECIPE \
    --task-digest "0" \
    --variant-config $VARIANT_CONFIG \
    --output-dir ./out

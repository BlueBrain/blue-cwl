
# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/747?rev=16"

ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885?rev=2"

# https://bbp.epfl.ch/nexus/v1/resources/bbp/mmb-point-neuron-framework-model/_/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1
ME_TYPE_DENSITIES="https://bbp.epfl.ch/neurosciencegraph/data/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1"

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/0a4f576a-39d9-4214-a474-a4ee049e43cf?rev=1"

export SALLOC_ACCOUNT=${SALLOC_ACCOUNT:-"proj30"}


rm -rf out && mkdir out

cwl-registry -vv execute neurons-me-type-property \
    --region $REGION \
    --variant-config $VARIANT_CONFIG \
    --atlas $ATLAS \
    --me-type-densities $ME_TYPE_DENSITIES \
    --nexus-base ${NEXUS_BASE:-"https://bbp.epfl.ch/nexus/v1"} \
    --nexus-org ${NEXUS_ORG:-"bbp"} \
    --nexus-project ${NEXUS_PROJ:-"mmb-point-neuron-framework-model"} \
    --nexus-token $NEXUS_TOKEN \
    --task-digest "0" \
    --output-dir ./out


ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885?rev=2"

# https://bbp.epfl.ch/nexus/v1/resources/bbp/mmb-point-neuron-framework-model/_/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1
ME_TYPE_DENSITIES="https://bbp.epfl.ch/neurosciencegraph/data/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1"

rm -rf out && mkdir out

cwl-registry -vv execute cell-composition-summary from-density-distribution \
    --atlas-release $ATLAS \
    --density-distribution $ME_TYPE_DENSITIES \
    --nexus-base ${NEXUS_BASE:-"https://bbp.epfl.ch/nexus/v1"} \
    --nexus-org ${NEXUS_ORG:-"bbp"} \
    --nexus-project ${NEXUS_PROJ:-"mmb-point-neuron-framework-model"} \
    --nexus-token $NEXUS_TOKEN \
    --task-digest "0" \
    --output-dir ./out

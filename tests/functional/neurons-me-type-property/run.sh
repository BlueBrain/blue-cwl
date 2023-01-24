
# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/322?rev=16"

ATLAS="https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885?rev=2"

# https://bbp.epfl.ch/nexus/v1/resources/bbp/mmb-point-neuron-framework-model/_/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1
ME_TYPE_DENSITIES="https://bbp.epfl.ch/neurosciencegraph/data/d6485af7-820c-465b-aacc-e1110b2b8d95?rev=1"

CELL_COMPOSITION="https://bbp.epfl.ch/neurosciencegraph/data/63ee3481-c4b6-4048-b91a-1871540a96b4?rev=2"

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/0a4f576a-39d9-4214-a474-a4ee049e43cf?rev=1"

rm -rf out && mkdir out

export NEXUS_BASE="https://bbp.epfl.ch/nexus/v1"
export NEXUS_PROJ="mmb-point-neuron-framework-model"
export NEXUS_ORG="bbp"

cwl-registry -vv execute neurons-me-type-property \
    --region $REGION \
    --variant-config $VARIANT_CONFIG \
    --atlas $ATLAS \
    --me-type-densities $CELL_COMPOSITION \
    --task-digest "0" \
    --output-dir ./out

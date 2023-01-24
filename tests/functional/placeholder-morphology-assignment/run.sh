# whole brain

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/aa2ff699-ebc6-45ef-9a5f-6a7d74fc2da6?rev=1"
rm -rf out && mkdir out

export NEXUS_BASE="https://bbp.epfl.ch/nexus/v1"
export NEXUS_PROJ="mmb-point-neuron-framework-model"
export NEXUS_ORG="bbp"

cwl-registry -vv execute placeholder-morphology-assignment \
    --region "http://api.brain-map.org/api/v2/data/Structure/997" \
    --mtype-morphologies "https://bbp.epfl.ch/neurosciencegraph/data/e31027cb-e0d1-4630-9b66-f1d433437ee4?rev=1" \
    --partial-circuit "https://bbp.epfl.ch/neurosciencegraph/data/a091fa51-a02a-401b-97cc-fd8c6e03b092?rev=1" \
    --task-digest "0" \
    --variant-config $VARIANT_CONFIG \
    --output-dir ./out

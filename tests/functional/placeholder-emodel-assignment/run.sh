# whole brain

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/f2befebe-5964-45c1-aa43-221a00bf444c?rev=1"

rm -rf out && mkdir out

export NEXUS_BASE="https://bbp.epfl.ch/nexus/v1"
export NEXUS_PROJ="mmb-point-neuron-framework-model"
export NEXUS_ORG="bbp"

cwl-registry -vv execute placeholder-emodel-assignment \
    --region "http://api.brain-map.org/api/v2/data/Structure/997" \
    --partial-circuit "https://bbp.epfl.ch/neurosciencegraph/data/31a77ce7-cfe7-4aed-9a36-0f7970311cc6?rev=1" \
    --etype-emodels "https://bbp.epfl.ch/neurosciencegraph/data/000ccb05-8518-47ff-b726-87ff3975e2da?rev=1" \
    --task-digest "0" \
    --variant-config $VARIANT_CONFIG \
    --output-dir ./out

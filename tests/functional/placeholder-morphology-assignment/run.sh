# whole brain

VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/aa2ff699-ebc6-45ef-9a5f-6a7d74fc2da6?rev=1"

# Primary somatosensory area (SSp)
REGION="http://api.brain-map.org/api/v2/data/Structure/322?rev=16"


VARIANT_CONFIG="https://bbp.epfl.ch/neurosciencegraph/data/eca1b2aa-0af9-4fe7-9492-bf7c0428f8a8?rev=1"
# URL: https://staging.nise.bbp.epfl.ch/nexus/v1/resources/bbp_test/studio_data_11/_/eca1b2aa-0af9-4fe7-9492-bf7c0428f8a8?rev=1

CIRCUIT="https://bbp.epfl.ch/neurosciencegraph/data/8aef7c10-6cf3-44ee-8e51-ddedb4f3dab5?rev=1"
# URL: https://staging.nise.bbp.epfl.ch/nexus/v1/resources/bbp_test/studio_data_11/_/8aef7c10-6cf3-44ee-8e51-ddedb4f3dab5?rev=1

# Production
# MTYPE_MORPHOLOGIES="https://bbp.epfl.ch/neurosciencegraph/data/e31027cb-e0d1-4630-9b66-f1d433437ee4?rev=1"

# Staging
MTYPE_MORPHOLOGIES="https://bbp.epfl.ch/neurosciencegraph/data/69b30218-ca93-4b77-881a-80fccde2af1e?rev=1"
# URL: https://staging.nise.bbp.epfl.ch/nexus/v1/resources/bbp_test/studio_data_11/_/69b30218-ca93-4b77-881a-80fccde2af1e?rev=1

cwl-registry -vv execute placeholder-morphology-assignment \
    --region $REGION \
    --mtype-morphologies $MTYPE_MORPHOLOGIES \
    --partial-circuit $CIRCUIT \
    --variant-config $VARIANT_CONFIG \
    --output-dir ./out



pushd out

cwl-workflow -vv me-type-property \
    --region "http://api.brain-map.org/api/v2/data/Structure/997" \
    --variant-config "https://bbp.epfl.ch/neurosciencegraph/data/e0f75f6b-6cb9-4c6d-a170-b9b0754e9c20?rev=1" \
    --me-type-densities "https://bbp.epfl.ch/neurosciencegraph/data/27652d4d-3a6f-42c7-9833-64396104c445?rev=4" \
    --atlas "https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885" \
    --nexus-base "https://bbp.epfl.ch/nexus/v1" \
    --nexus-project "mmb-point-neuron-framework-model" \
    --nexus-org bbp \
    --nexus-token $NEXUS_TOKEN \
    --task-digest "532eaabd9574880dbf76b9b8cc00832c20a6ec113d682299550d7a6e0f345e25" \
    --output-dir .

popd

# whole brain

pushd out

cwl-workflow -vv placeholder-morphology-assignment \
    --region "http://api.brain-map.org/api/v2/data/Structure/997" \
    --mtype-morphologies "https://bbp.epfl.ch/neurosciencegraph/data/e31027cb-e0d1-4630-9b66-f1d433437ee4?rev=1" \
    --partial-circuit "https://bbp.epfl.ch/neurosciencegraph/data/a091fa51-a02a-401b-97cc-fd8c6e03b092?rev=1" \
    --nexus-base "https://bbp.epfl.ch/nexus/v1" \
    --nexus-project "mmb-point-neuron-framework-model" \
    --nexus-org bbp \
    --nexus-token $NEXUS_TOKEN \
    --task-digest "532eaabd9574880dbf76b9b8cc00832c20a6ec113d682299550d7a6e0f345e25" \
    --output-dir .

popd

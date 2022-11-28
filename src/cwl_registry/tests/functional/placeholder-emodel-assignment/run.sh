# whole brain

pushd out

cwl-workflow -vv placeholder-emodel-assignment \
    --region "http://api.brain-map.org/api/v2/data/Structure/997" \
    --partial-circuit "https://bbp.epfl.ch/neurosciencegraph/data/31a77ce7-cfe7-4aed-9a36-0f7970311cc6?rev=1" \
    --etype-emodels "https://bbp.epfl.ch/neurosciencegraph/data/000ccb05-8518-47ff-b726-87ff3975e2da?rev=1" \
    --nexus-base "https://bbp.epfl.ch/nexus/v1" \
    --nexus-project "mmb-point-neuron-framework-model" \
    --nexus-org bbp \
    --nexus-token $NEXUS_TOKEN \
    --task-digest "532eaabd9574880dbf76b9b8cc00832c20a6ec113d682299550d7a6e0f345e25" \
    --output-dir .

popd



pushd out

cwl-workflow -vv me-type-property \
    --region "http://api.brain-map.org/api/v2/data/Structure/315" \
    --variant-config "https://bbp.epfl.ch/neurosciencegraph/data/e7967057-25b0-48d3-9b8f-c82e587ab687?rev=1" \
    --mtype-densities "https://bbp.epfl.ch/neurosciencegraph/data/0b8b8a6c-bb2c-48d6-a812-5f89b1b86adf?rev=1" \
    --etype-ratios "https://bbp.epfl.ch/neurosciencegraph/data/4765f783-a4d8-4b3e-92ec-55ef12fbaec7?rev=1" \
    --atlas "https://bbp.epfl.ch/neurosciencegraph/data/831a626a-c0ae-4691-8ce8-cfb7491345d9" \
    --nexus-base "https://bbp.epfl.ch/nexus/v1" \
    --nexus-project "mmb-point-neuron-framework-model" \
    --nexus-org bbp \
    --nexus-token $NEXUS_TOKEN \
    --task-digest "532eaabd9574880dbf76b9b8cc00832c20a6ec113d682299550d7a6e0f345e25" \
    --output-dir .

popd

cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'neurons-me-type-property']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv-config
  enable_internet: true


resources:
    default:
        partition: prod_small
        nodes: 1
        exclusive: true
        time: '1:00:00'
        ntasks: 1
        ntasks_per_node: 1
        cpus_per_task: 1
    region:
        'http://api.brain-map.org/api/v2/data/Structure/997':
            partition: prod
            time: '2:00:00'


inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: atlas
      type: NexusType
      inputBinding:
        prefix: --atlas

    - id: me_type_densities
      type: NexusType
      inputBinding:
        prefix: --me-type-densities

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: circuit_me_type_bundle
      type: NexusType
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "me-type-property-partial-circuit.json"

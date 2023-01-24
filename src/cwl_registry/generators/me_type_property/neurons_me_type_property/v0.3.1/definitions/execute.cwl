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

cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator

baseCommand: ['blue-cwl', 'execute', 'neurons-cell-position']


environment:
  env_type: MODULE
  modules:
    - unstable
    - brainbuilder
    - py-blue-cwl
  enable_internet: true

executor:
  type: slurm
  slurm_config:
    partition: prod
    nodes: 1
    exclusive: true
    time: '8:00:00'
    account: proj134
  remote_config:
    host: bbpv1.epfl.ch
  env_vars:
    FOO: foo
    BAR: bar

inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: cell_composition
      type: NexusType
      inputBinding:
        prefix: --cell-composition

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: partial_circuit
      type: NexusType
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "partial-circuit.json"

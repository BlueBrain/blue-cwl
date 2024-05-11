cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'neurons-cell-position', 'mono-execution']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311
  enable_internet: true

executor:
  type: slurm
  slurm_config:
    partition: prod
    nodes: 1
    exclusive: true
    time: '2:00:00'
    account: proj134
  remote_config:
    host: bbpv1.epfl.ch

inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: cell_composition
      type: NexusType
      inputBinding:
        prefix: --cell-composition-id

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration-id

    - id: variant
      type: NexusType
      inputBinding:
        prefix: --variant-id

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: partial_circuit
      type: NexusType
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "$(inputs.output_dir.path)/partial-circuit.json"

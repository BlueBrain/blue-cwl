cwlVersion: v1.2
class: CommandLineTool

id: me-model-register
label: me-model-register

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '1:00:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "me-model", "register"]

inputs:

  - id: circuit_id
    type: NexusType
    inputBinding:
      prefix: --circuit-id

  - id: circuit_file
    type: File
    inputBinding:
      prefix: --circuit-file

  - id: nodes_file
    type: File
    inputBinding:
      prefix: --nodes-file

  - id: hoc_dir
    type: Directory
    inputBinding:
      prefix: --hoc-dir

  - id: output_dir
    type: Directory
    inputBinding:
      prefix: --output-dir

  - id: output_resource_file
    type: string
    inputBinding:
      prefix: --output-resource-file

outputs:

  - id: circuit
    type: NexusType
    outputBinding:
      glob: $(inputs.output_resource_file)

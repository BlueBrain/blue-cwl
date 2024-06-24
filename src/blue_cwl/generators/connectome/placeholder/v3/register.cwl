cwlVersion: v1.2
class: CommandLineTool

id: connectome-filtering-register
label: connectome-filtering-register

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

baseCommand: ["blue-cwl", "execute", "connectome-generation-placeholder", "register"]

inputs:

  - id: circuit_id
    type: NexusType
    inputBinding:
      prefix: --circuit-id

  - id: edges_file
    type: File
    inputBinding:
      prefix: --edges-file

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

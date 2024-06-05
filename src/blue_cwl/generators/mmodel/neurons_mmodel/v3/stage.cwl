cwlVersion: v1.2
class: CommandLineTool

id: stage_nexus_resources
label: stage-nexus-resources

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '00:10:00'
    ntasks: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "mmodel", "stage"]

inputs:

  - id: configuration_id
    type: NexusType
    inputBinding:
      prefix: --configuration-id

  - id: circuit_id
    type: NexusType
    inputBinding:
      prefix: --circuit-id

  - id: stage_dir
    type: Directory
    inputBinding:
      prefix: --stage-dir

outputs:

  - id: atlas_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/atlas.json

  - id: atlas_dir
    type: Directory
    outputBinding:
      glob: $(inputs.stage_dir.path)/atlas

  - id: circuit_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/circuit_config.json

  - id: nodes_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/nodes.h5

  - id: canonical_config_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/canonical_config.json

  - id: placeholder_config_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/placeholder_config.json

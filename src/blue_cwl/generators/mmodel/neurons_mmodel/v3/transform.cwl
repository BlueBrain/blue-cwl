cwlVersion: v1.2
class: CommandLineTool

id: transform
label: transform

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

baseCommand: ["blue-cwl", "execute", "mmodel-neurons", "transform"]

inputs:

  - id: atlas_file
    type: File
    inputBinding:
      prefix: --atlas-file

  - id: canonical_config_file
    type: File
    inputBinding:
      prefix: --canonical-config-file

  - id: transform_dir
    type: Directory
    inputBinding:
      prefix: --transform-dir

outputs:

  - id: region_file
    type: File
    outputBinding:
      glob: $(inputs.transform_dir.path)/region_structure.yml

  - id: parameters_file
    type: File
    outputBinding:
      glob: $(inputs.transform_dir.path)/tmd_parameters.json

  - id: distributions_file
    type: File
    outputBinding:
      glob: $(inputs.transform_dir.path)/tmd_distributions.json

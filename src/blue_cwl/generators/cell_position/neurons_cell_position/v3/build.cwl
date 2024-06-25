cwlVersion: v1.2
class: CommandLineTool

id: neurons_cell_position_transform
label: neurons-cell-position-transform

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

baseCommand: ["blue-cwl", "execute", "neurons-cell-position", "build"]

inputs:

  - id: atlas_file
    type: File
    inputBinding:
      prefix: --atlas-file

  - id: region_file
    type: File
    inputBinding:
      prefix: --region-file

  - id: densities_file
    type: File
    inputBinding:
      prefix: --densities-file

  - id: configuration_file
    type: File
    inputBinding:
      prefix: --configuration-file

  - id: composition_file
    type: File
    inputBinding:
      prefix: --composition-file

  - id: mtype_taxonomy_file
    type: File
    inputBinding:
      prefix: --mtype-taxonomy-file

  - id: build_dir
    type: Directory
    inputBinding:
      prefix: --build-dir

outputs:

  - id: circuit_file
    type: File
    outputBinding:
      glob: $(inputs.build_dir.path)/circuit_config.json

  - id: summary_file
    type: File
    outputBinding:
      glob: $(inputs.build_dir.path)/cell_composition_summary.json


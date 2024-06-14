cwlVersion: v1.2
class: CommandLineTool

id: stage_cell_composition_manipulation
label: stage-cell-composition-manipulation

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '01:00:00'
    ntasks: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "cell-composition-manipulation", "stage"]

inputs:

  - id: configuration_id
    type: NexusType
    inputBinding:
      prefix: --configuration-id

  - id: base_cell_composition_id
    type: NexusType
    inputBinding:
      prefix: --base-cell-composition-id

  - id: stage_dir
    type: Directory
    inputBinding:
      prefix: --stage-dir

outputs:

  - id: atlas_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/atlas.json

  - id: recipe_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/recipe.parquet

  - id: region_selection_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/region_selection.json

  - id: densities_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/cell_composition_volume.json

  - id: materialized_densities_file
    type: File
    outputBinding:
      glob: $(inputs.stage_dir.path)/cell_composition_volume.parquet

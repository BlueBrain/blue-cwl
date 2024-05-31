cwlVersion: v1.2
class: CommandLineTool

id: emogel_recipe
label: emodel-recipe

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
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["blue-cwl", "execute", "me-model", "recipe"]

inputs:

  - id: config_file
    type: File
    inputBinding:
      prefix: --config-file

  - id: output_file
    type: string
    inputBinding:
      prefix: --output-file

outputs:

  - id: recipe_file
    type: File
    outputBinding:
      glob: $(inputs.output_file)

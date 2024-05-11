cwlVersion: v1.2
class: CommandLineTool

id: connectome-filtering-transform
label: connectome-filtering-transform

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

baseCommand: ["cwl-registry", "execute", "connectome-filtering-synapses", "recipe"]

inputs:

  - id: circuit_file
    type: File
    inputBinding:
      prefix: --circuit-file

  - id: source_node_population_name
    type: string
    inputBinding:
      prefix: --source-node-population-name

  - id: target_node_population_name
    type: string
    inputBinding:
      prefix: --target-node-population-name

  - id: atlas_file
    type: File
    inputBinding:
      prefix: "--atlas-file"

  - id: configuration_file
    type: File
    inputBinding:
      prefix: "--configuration-file"

  - id: output_recipe_file
    type: string
    inputBinding:
      prefix: "--output-recipe-file"

outputs:

  - id: recipe_file
    type: File
    outputBinding:
      glob: $(inputs.output_recipe_file)

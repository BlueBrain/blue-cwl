cwlVersion: v1.2
class: CommandLineTool

id: connectome-filtering-staging
label: connectome-filtering-staging

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

baseCommand: ["blue-cwl", "execute", "connectome-filtering-synapses", "stage"]


inputs:

  - id: configuration
    type: NexusType
    inputBinding:
      prefix: "--configuration-id"

  - id: circuit
    type: NexusType
    inputBinding:
      prefix: "--circuit-id"

  - id: variant
    type: NexusType
    inputBinding:
      prefix: "--variant-id"

  - id: stage_dir
    type: Directory
    inputBinding:
      prefix: "--staging-dir"

  - id: output_configuration_file
    type: string
    inputBinding:
      prefix: "--output-configuration-file"

  - id: output_circuit_file
    type: string
    inputBinding:
      prefix: "--output-circuit-file"

  - id: output_variant_file
    type: string
    inputBinding:
      prefix: "--output-variant-file"

  - id: output_atlas_file
    type: string
    inputBinding:
      prefix: "--output-atlas-file"

  - id: output_edges_file
    type: string
    inputBinding:
      prefix: "--output-edges-file"

outputs:

  - id: configuration_file
    type: File
    outputBinding:
      glob: $(inputs.output_configuration_file)

  - id: circuit_file
    type: File
    outputBinding:
      glob: $(inputs.output_circuit_file)

  - id: variant_file
    type: File
    outputBinding:
      glob: $(inputs.output_variant_file)

  - id: atlas_file
    type: File
    outputBinding:
      glob: $(inputs.output_atlas_file)

  - id: edges_file
    type: File
    outputBinding:
      glob: $(inputs.output_edges_file)

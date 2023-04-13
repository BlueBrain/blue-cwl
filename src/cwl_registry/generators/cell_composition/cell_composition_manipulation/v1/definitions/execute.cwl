cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'cell-composition-manipulation']


environment:
  env_type: MODULE
  modules:
    - unstable
    - py-cwl-registry/0.3.6
  enable_internet: true


inputs:

    - id: region
      type: NexusType
      inputBinding:
        prefix: --region

    - id: base_cell_composition
      type: NexusType
      inputBinding:
        prefix: --base-cell-composition

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: cell_composition
      type: NexusType
      outputBinding:
        glob: "output-density-distribution.json"

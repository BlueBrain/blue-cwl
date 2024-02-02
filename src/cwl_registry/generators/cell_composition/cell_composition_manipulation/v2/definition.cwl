cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'cell-composition-manipulation']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/data/scratch/proj134/zisis/composition-selection/venv310/bin/activate
  enable_internet: true


inputs:

    - id: region
      type: NexusType
      inputBinding:
        prefix: --region

    - id: brain_region_selector_config
      type: NexusType
      inputBinding:
        prefix: --brain-region-selector-config

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

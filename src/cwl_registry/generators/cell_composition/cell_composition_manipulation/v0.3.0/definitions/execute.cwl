cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'cell-composition-manipulation']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv-densities
  enable_internet: true


inputs:

    - id: region
      type: NexusType
      inputBinding:
        prefix: --region

    - id: base_composition_summary
      type: NexusType
      inputBinding:
        prefix: --base-composition-summary

    - id: base_density_distribution
      type: NexusType
      inputBinding:
        prefix: --base-density-distribution

    - id: atlas_release
      type: NexusType
      inputBinding:
        prefix: --atlas-release

    - id: recipe
      type: NexusType
      inputBinding:
        prefix: --recipe

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: cell_composition_manipulation__density_distribution
      type: NexusType
      outputBinding:
        glob: "density-distribution.json"

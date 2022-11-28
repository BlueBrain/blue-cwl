cwlVersion: v1.2
class: CommandLineTool

id: me_type_property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-workflow', 'me-type-property', 'execute']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj30/tickets/NSETM-1760-wrap-snakemake-with-luigi/bbp-workflow-venv


inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: atlas
      type: NexusType
      inputBinding:
        prefix: --atlas

    - id: mtype_densities
      type: NexusType
      inputBinding:
        prefix: --mtype-densities

    - id: etype_ratios
      type: NexusType
      inputBinding:
        prefix: --etype-ratios

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: nexus_base
      type: string
      inputBinding:
        prefix: --nexus-base

    - id: nexus_token
      type: string
      inputBinding:
        prefix: --nexus-token

    - id: nexus_org
      type: string
      inputBinding:
        prefix: --nexus-org

    - id: nexus_project
      type: string
      inputBinding:
        prefix: --nexus-project

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: circuit_me_type_bundle
      type: NexusType
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "me-type-property-partial-circuit.json"

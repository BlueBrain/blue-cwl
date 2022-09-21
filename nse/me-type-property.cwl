

cwlVersion: v1.2
class: CommandLineTool

id: me-type-property
label: Morph-Electric type property generator
stdout: stdout.txt

baseCommand: ['cwl-workflow', 'me-type-property']

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj30/tickets/NSETM-1760-wrap-snakemake-with-luigi/bbp-workflow-venv

inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: mtype-taxonomy
      type: File
      inputBinding:
        prefix: --mtype-taxonomy-file

    - id: cell-composition
      type: File
      inputBinding:
        prefix: --cell-composition-file

    - id: atlas
      type: Directory
      inputBinding:
        prefix: --atlas-directory

outputs:

    - id: circuit-me-type-bundle
      type: File
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "config.json"

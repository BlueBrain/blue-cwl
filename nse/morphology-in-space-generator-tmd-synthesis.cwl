
cwlVersion: v1.2
class: CommandLineTool

id: morphology-in-space-tmd-synthesis
label: Morphology in space generator using tmd synthesis for dendrites and release of axons.
stdout: stdout.txt

baseCommand: ['cwl-workflow', '-vv', 'morphology-in-space', 'tmd-synthesis']

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj30/tickets/NSETM-1760-wrap-snakemake-with-luigi/bbp-workflow-venv

inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: circuit-me-type-bundle
      type: File
      inputBinding:
        prefix: --circuit-me-type-bundle

    - id: atlas-directory
      doc: Atlas directory that must include distances, and orientations.
      type: Directory
      inputBinding:
        prefix: --atlas-directory

    - id: morphology-release
      doc: Morphology release to choose axons from.
      type: Directory
      inputBinding:
        prefix: --morphology-release

    - id: placement-rules
      type: File
      inputBinding:
        prefix: --placement-rules

    - id: neurondb-axon
      type: File
      inputBinding:
        prefix: --neurondb-axon

    - id: tmd-parameters
      doc: Topolological morphology decriptor parameters for dendrite synthesis.
      type: File
      inputBinding:
        prefix: --tmd-parameters

    - id: tmd-distributions
      doc: Topolological morphology decriptor distributions for dendrite synthesis.
      type: File
      inputBinding:
        prefix: --tmd-distributions

outputs:

    - id: circuit-synthesized-morphologies-bundle
      type: File
      doc: Circuit bundle with me-types and soma positions.
      outputBinding:
        glob: "config.json"

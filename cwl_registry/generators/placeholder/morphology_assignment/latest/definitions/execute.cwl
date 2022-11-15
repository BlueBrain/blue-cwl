cwlVersion: v1.2
class: CommandLineTool

id: placeholder_morphology_assignment
label: Morphology Assignment
stdout: stdout.txt

baseCommand: ['cwl-workflow', 'placeholder-morphology-assignment']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj30/tickets/NSETM-1760-wrap-snakemake-with-luigi/workflow-example/venv


inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: partial_circuit
      type: NexusType
      inputBinding:
        prefix: --partial-circuit

    - id: mtype_morphologies
      type: NexusType
      inputBinding:
        prefix: --mtype-morphologies

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

    - id: "circuit_morphologies_bundle"
      type: NexusType
      doc: Circuit bundle with me-types and morphologies.
      outputBinding:
        glob: "circuit_config.json"

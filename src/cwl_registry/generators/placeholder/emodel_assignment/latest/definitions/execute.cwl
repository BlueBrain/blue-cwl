cwlVersion: v1.2
class: CommandLineTool

id: placeholder_emodel_assignment
label: EModel assignment
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'placeholder-emodel-assignment']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv-densities


inputs:

    - id: region
      type: string
      inputBinding:
        prefix: --region

    - id: partial_circuit
      type: NexusType
      inputBinding:
        prefix: --partial-circuit

    - id: etype_emodels
      type: NexusType
      inputBinding:
        prefix: --etype-emodels

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

    - id: "circuit_emodels_bundle"
      type: NexusType
      doc: Circuit bundle with emodels.
      outputBinding:
        glob: "circuit_config.json"

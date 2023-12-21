cwlVersion: v1.2
class: CommandLineTool

id: placeholder_morphology_assignment
label: Morphology Assignment
stdout: stdout.txt

baseCommand: ['cwl-registry', 'execute', 'me-model', 'mono-execution']


environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/workflows/environments/venv310-memodel
  enable_internet: true


resources:
    default:
        partition: prod
        account: proj134
        exclusive: true
        time: '8:00:00'
        ntasks: 1440
        mem: 0

    sub-tasks:

      # emodel-generalisation assign
      - partition: prod
        account: proj134
        exclusive: true
        time: '1:00:00'
        nodes: 1
        mem: 0

      # emodel-generalisation adapt
      - partition: prod
        account: proj134
        exclusive: true
        time: '11:00:00'
        constraint: nvme
        ntasks: 1000
        mem: 0

      # emodel-generalisation currents
      - partition: prod
        account: proj134
        exclusive: true
        time: '12:00:00'
        constraint: nvme
        ntasks: 1000
        mem: 0


inputs:

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration

    - id: partial_circuit
      type: NexusType
      inputBinding:
        prefix: --partial-circuit

    - id: variant_config
      type: NexusType
      inputBinding:
        prefix: --variant-config

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: partial_circuit
      type: NexusType
      doc: Circuit bundle with me-types and morphologies.
      outputBinding:
        glob: "circuit_me_model_bundle.json"

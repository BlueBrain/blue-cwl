cwlVersion: v1.2
class: CommandLineTool

id: placeholder_morphology_assignment
label: Morphology Assignment
stdout: stdout.txt

baseCommand: ['blue-cwl', 'execute', 'mmodel-neurons']


environment:
  env_type: MODULE
  modules:
    - unstable
    - py-blue-cwl
    - py-region-grower
  enable_internet: true


resources:
    default:
        partition: prod
        account: proj134
        exclusive: true
        time: '1-0:00:00'
        ntasks: 400
        cpus_per_task: 2
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
        glob: "circuit_morphologies_bundle.json"

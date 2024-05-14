cwlVersion: v1.2
class: CommandLineTool

id: neurons_morphology_synthesis
label: Morphology assignment of synthesized morphologies
stdout: stdout.txt

baseCommand: ['blue-cwl', 'execute', 'mmodel-neurons', 'mono-execution']

environment:
  env_type: VENV
  path: /gpfs/bbp.cscs.ch/project/proj134/scratch/zisis/sub-workflows/venv311
  enable_internet: true

executor:
  type: slurm
  slurm_config:
    partition: prod
    account: proj134
    exclusive: true
    time: '1-0:00:00'
    nodes: 1
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

# temporary workfaround to run synthesis
# TODO: Migrate to a Workflow with proper allocations per step
resources:
    default:
        partition: prod
        account: proj134
        exclusive: true
        time: '1-0:00:00'
        ntasks: 200
        cpus_per_task: 2
        mem: 0

inputs:

    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration-id

    - id: circuit
      type: NexusType
      inputBinding:
        prefix: --circuit-id

    - id: variant
      type: NexusType
      inputBinding:
        prefix: --variant-id

    - id: output_dir
      type: Directory
      inputBinding:
        prefix: --output-dir

outputs:

    - id: partial_circuit
      type: NexusType
      doc: Circuit bundle with me-types and morphologies.
      outputBinding:
        glob: "$(inputs.output_dir.path)/resource.json"

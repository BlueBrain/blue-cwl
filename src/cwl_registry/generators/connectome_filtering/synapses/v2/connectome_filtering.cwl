cwlVersion: v1.2
class: CommandLineTool

id: connectome-filtering-functionalizer
label: connectome-filtering-functionalizer

environment:
  env_type: MODULE
  modules:
    - unstable
    - spykfunc

executor:
  type: slurm
  slurm_config:
    partition: prod
    nodes: 5
    exclusive: true
    time: '8:00:00'
    account: proj134
    constraint: nvme
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch
  env_vars:
    PYARROW_IGNORE_TIMEZONE: '1'

baseCommand: ["dplace", "functionalizer"]

inputs:

  - id: edges_file
    type: File
    inputBinding:
      position: 1

  - id: circuit_config
    type: File
    inputBinding:
      position: 2
      prefix: "--circuit-config"

  - id: work_dir
    type: string
    inputBinding:
      position: 3
      prefix: "--work-dir"

  - id: output_dir
    type: string
    inputBinding:
      position: 4
      prefix: "--output-dir"

  - id: from
    type: string
    inputBinding:
      position: 5
      prefix: "--from"

  - id: to
    type: string
    inputBinding:
      position: 6
      prefix: "--to"

  - id: filters
    type: string[]
    inputBinding:
      position: 7
      prefix: "--filters"
      itemSeparator: " "

  - id: recipe
    type: File
    inputBinding:
      position: 6
      prefix: "--recipe"

outputs:
    - id: parquet_dir
      type: Directory
      outputBinding:
        glob: $(inputs.output_dir)/circuit.parquet

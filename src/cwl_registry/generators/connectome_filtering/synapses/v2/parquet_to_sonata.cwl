cwlVersion: v1.2
class: CommandLineTool

id: connectome-filtering-transform
label: connectome-filtering-transform

environment:
  env_type: MODULE
  modules:
  - unstable
  - parquet-converters

executor:
  type: slurm
  slurm_config:
    partition: prod
    nodes: 3
    ntasks_per_node: 10
    exclusive: true
    time: '8:00:00'
    account: proj134
    constraint: nvme
    mem: 0
  remote_config:
    host: bbpv1.epfl.ch

baseCommand: ["parquet2hdf5"]

inputs:

  - id: parquet_dir
    type: Directory
    inputBinding:
      position: 0

  - id: output_edges_file
    type: string
    inputBinding:
      position: 1

  - id: output_edge_population_name
    type: string
    inputBinding:
      position: 2

outputs:

  - id: edges_file
    type: File
    outputBinding:
      glob: $(inputs.output_edges_file)

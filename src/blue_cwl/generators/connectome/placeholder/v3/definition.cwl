cwlVersion: v1.2
class: Workflow

id: connectome_generation_placeholder
label: Connectome generation using placeholder algorithm


inputs:

  - id: configuration_id
    type: NexusType

  - id: macro_connectome_config_id
    type: NexusType

  - id: circuit_id
    type: NexusType

  - id: output_dir
    type: Directory

outputs:

  - id: circuit
    type: NexusType
    doc: Circuit bundle with connectivity
    outputSource: register/circuit

steps:

  - id: setup
    run: ./setup.cwl
    in:
      output_dir: output_dir
    out:
      - stage_dir
      - build_dir
      - transform_dir

  - id: stage
    run: ./stage.cwl
    in:
      circuit_id: circuit_id
      stage_dir: setup/stage_dir
      configuration_id: configuration_id
      macro_connectome_config_id: macro_connectome_config_id
    out:
      - atlas_file
      - circuit_file
      - macro_config_file
      - micro_config_file

  - id: transform
    run: ./transform.cwl
    in:
      atlas_file: stage/atlas_file
      circuit_file: stage/circuit_file
      macro_config_file: stage/macro_config_file
      micro_config_file: stage/micro_config_file
      transform_dir: setup/transform_dir
    out:
      - recipe_file

  - id: generate_connectome
    run: ./generate_connectome.cwl
    in:
      recipe_file: transform/recipe_file
      output_dir: setup/build_dir
    out:
      - parquet_dir

  - id: parquet_to_sonata
    run: ./parquet_to_sonata.cwl
    in:
      parquet_dir: generate_connectome/parquet_dir
      output_edges_file:
        source: setup/build_dir
        valueFrom: $(self.path)/edges.h5
      output_edge_population_name:
        valueFrom: root__neurons__root_neurons__chemical
    out:
      - edges_file

  - id: register
    run: ./register.cwl
    in:
      output_dir: output_dir
      circuit_id: circuit_id
      edges_file: parquet_to_sonata/edges_file
      output_resource_file:
        source: output_dir
        valueFrom: $(self.path)/resource.json
    out:
      - circuit

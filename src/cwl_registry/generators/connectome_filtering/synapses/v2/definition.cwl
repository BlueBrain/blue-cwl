cwlVersion: v1.2
class: Workflow

id: workflow_me_type_property
label: mock-generator-workflow

inputs:

    - id: configuration
      type: NexusType

    - id: circuit
      type: NexusType

    - id: variant
      type: NexusType

    - id: output_dir
      type: Directory


outputs:
  partial_circuit: 
      type: NexusType
      outputSource: register/circuit

steps:

  - id: dirs
    run: ./dirs.cwl
    in:
      output_stage_dir:
        source: output_dir
        valueFrom: $(self.path)/stage
      output_build_dir:
        source: output_dir
        valueFrom: $(self.path)/build
    out:
      - build_dir
      - stage_dir

  - id: stage
    run: ./staging.cwl
    in:
      configuration: configuration
      circuit: circuit
      variant: variant
      stage_dir:
        source: dirs/stage_dir
      output_configuration_file:
        source: dirs/stage_dir
        valueFrom: $(self.path)/staged_configuration_file.json
      output_circuit_file:
        source: dirs/stage_dir
        valueFrom: $(self.path)/circuit_config.json
      output_variant_file:
        source: dirs/stage_dir
        valueFrom: $(self.path)/variant.cwl
      output_atlas_file:
        source: dirs/stage_dir
        valueFrom: $(self.path)/atlas.json
      output_edges_file:
        source: dirs/stage_dir
        valueFrom: $(self.path)/edges.h5
    out:
      - configuration_file
      - circuit_file
      - variant_file
      - atlas_file
      - edges_file

  - id: transform
    run: ./transform.cwl
    in:
      circuit_file:
        source: stage/circuit_file
      source_node_population_name:
        valueFrom: root__neurons
      target_node_population_name:
        valueFrom: root__neurons
      atlas_file:
        source: stage/atlas_file
      configuration_file:
        source: stage/configuration_file
      output_recipe_file:
        source: dirs/build_dir
        valueFrom: $(self.path)/recipe.json
    out:
      - recipe_file

  - id: connectome_filtering
    run: ./connectome_filtering.cwl
    in:
      edges_file:
        source: stage/edges_file
      circuit_config:
        source: stage/circuit_file
      output_dir:
        source: dirs/build_dir
      work_dir:
        source: dirs/build_dir
        valueFrom: $(self.path)/workdir
      from:
        valueFrom: root__neurons
      to:
        valueFrom: root__neurons
      filters:
        valueFrom:
          - SynapseProperties
      recipe:
        source: transform/recipe_file
    out:
      - parquet_dir

  - id: parquet_to_sonata
    run: ./parquet_to_sonata.cwl
    in:
      parquet_dir:
        source: connectome_filtering/parquet_dir
      output_edges_file:
        source: dirs/build_dir
        valueFrom: $(self.path)/edges.h5
      output_edge_population_name:
        valueFrom: root_neurons__root_neurons__chemical
    out:
      - edges_file

  - id: register
    run: ./register.cwl
    in:
      circuit: circuit
      output_dir:
        source: dirs/build_dir
      edges_file:
        source: parquet_to_sonata/edges_file
      output_resource_file:
        source: output_dir
        valueFrom: $(self.path)/resource.json
    out:
      - circuit

cwlVersion: v1.2
class: Workflow

id: workflow_neurons_cell_position
label: workflow-neurons-cell-position

inputs:

  - id: region_id
    type: NexusType

  - id: cell_composition_id
    type: NexusType

  - id: configuration_id
    type: NexusType

  - id: output_dir
    type: Directory

outputs:

  - id: circuit
    type: NexusType
    outputSource: register/circuit

steps:

  - id: setup
    in:
      output_dir: output_dir
    out:
      - stage_dir
      - build_dir
      - transform_dir
    run: ./setup.cwl

  - id: stage
    run: ./stage.cwl
    in:
      region_id: region_id
      stage_dir: setup/stage_dir
      configuration_id: configuration_id
      cell_composition_id: cell_composition_id
    out:
      - atlas_file
      - region_file
      - densities_file
      - configuration_file

  - id: transform
    run: ./transform.cwl
    in:
      region_file: stage/region_file
      densities_file: stage/densities_file
      transform_dir: setup/transform_dir
    out:
      - mtype_taxonomy_file
      - mtype_composition_file

  - id: build
    run: ./build.cwl
    in:
      atlas_file: stage/atlas_file
      build_dir: setup/build_dir
      region_file: stage/region_file
      densities_file: stage/densities_file
      configuration_file: stage/configuration_file
      composition_file: transform/mtype_composition_file
      mtype_taxonomy_file: transform/mtype_taxonomy_file
    out:
      - circuit_file
      - summary_file

  - id: register
    run: ./register.cwl
    in:
      region_id: region_id
      cell_composition_id: cell_composition_id
      circuit_file: build/circuit_file
      summary_file: build/summary_file
      output_dir: output_dir
      output_resource_file:
        source: output_dir
        valueFrom: $(self.path)/resource.json
    out:
      - circuit

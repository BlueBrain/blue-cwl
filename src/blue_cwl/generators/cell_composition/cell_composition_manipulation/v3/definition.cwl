cwlVersion: v1.2
class: Workflow

id: cell_composition_manipulation_workflow
label: CellComposition manipulation workflow


inputs:

  - id: configuration_id
    type: NexusType

  - id: base_cell_composition_id
    type: NexusType

  - id: output_dir
    type: Directory

outputs:

  - id: cell_composition
    type: NexusType
    doc: Manipulated CellComposition
    outputSource: register/cell_composition

steps:

  - id: setup
    run: ./setup.cwl
    in:
      output_dir: output_dir
    out:
      - stage_dir
      - build_dir

  - id: stage
    run: ./stage.cwl
    in:
      stage_dir: setup/stage_dir
      configuration_id: configuration_id
      base_cell_composition_id: base_cell_composition_id
    out:
      - atlas_file
      - recipe_file
      - region_selection_file
      - densities_file
      - materialized_densities_file

  - id: manipulate_cell_composition
    run: ./manipulate_cell_composition.cwl
    in:
      atlas_file: stage/atlas_file
      recipe_file: stage/recipe_file
      region_selection_file: stage/region_selection_file
      densities_file: stage/densities_file
      materialized_densities_file: stage/materialized_densities_file
      output_dir: setup/build_dir
    out:
      - cell_composition_volume_file
      - cell_composition_summary_file

  - id: register
    run: ./register.cwl
    in:
      base_cell_composition_id: base_cell_composition_id
      cell_composition_volume_file: manipulate_cell_composition/cell_composition_volume_file
      cell_composition_summary_file: manipulate_cell_composition/cell_composition_summary_file
      output_dir: output_dir
    out:
      - cell_composition

cwlVersion: v1.2
class: Workflow

id: workflow_me_model
label: workflow-me-model

inputs:

  - id: configuration_id
    type: NexusType

  - id: circuit_id
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
    run: ./setup.cwl

  - id: stage
    in:
      stage_dir: setup/stage_dir
      circuit_id: circuit_id
      configuration_id: configuration_id
    out:
      - config_file
      - circuit_file
      - nodes_file
      - morphologies_dir
    run: ./stage.cwl

  - id: recipe
    run: ./recipe.cwl
    in:
      config_file: stage/config_file
      output_file:
        source: setup/build_dir
        valueFrom: $(self.path)/recipe.json
    out:
      - recipe_file

  - id: emodel_prepare
    run: ./emodel_prepare.cwl
    in:
      config_file: recipe/recipe_file
      work_dir:
        source: setup/build_dir
        valueFrom: $(self.path)/configs
      out_mechanisms_dir:
        source: setup/build_dir
        valueFrom: $(self.path)/mechanisms
    out:
      - work_dir
      - mechanisms_dir

  - id: emodel_assign
    run: ./emodel_assign.cwl
    in:
      nodes_file: stage/nodes_file
      recipe_file: recipe/recipe_file
      out_nodes_file:
        source: setup/build_dir
        valueFrom: $(self.path)/assign_nodes.h5
      out_configs_dir:
        source: setup/build_dir
        valueFrom: $(self.path)/configs
    out:
      - nodes_file
      - configs_dir

  - id: emodel_adapt
    run: ./emodel_adapt.cwl
    in:
      nodes_file: emodel_assign/nodes_file
      recipe_file: recipe/recipe_file
      configs_dir: emodel_assign/configs_dir
      mechanisms_dir: emodel_prepare/mechanisms_dir
      morphologies_dir: stage/morphologies_dir
      work_dir:
        source: setup/build_dir
        valueFrom: $(self.path)/adapt_workdir
      out_nodes_file:
        source: setup/build_dir
        valueFrom: $(self.path)/adapt_nodes.h5
      out_hoc_dir:
        source: setup/build_dir
        valueFrom: $(self.path)/hoc
      parallel_lib:
        valueFrom: multiprocessing
    out:
      - hoc_dir
      - nodes_file

  - id: emodel_currents
    run: ./emodel_currents.cwl
    in:
      hoc_dir: emodel_adapt/hoc_dir
      recipe_file: recipe/recipe_file
      circuit_file: stage/circuit_file
      nodes_file: emodel_adapt/nodes_file
      morphologies_dir: stage/morphologies_dir
      mechanisms_dir: emodel_prepare/mechanisms_dir
      out_nodes_file:
        source: setup/build_dir
        valueFrom: $(self.path)/nodes.h5
      parallel_lib:
        valueFrom: multiprocessing
    out:
      - nodes_file

  - id: register
    run: ./register.cwl
    in:
      circuit_id: circuit_id
      circuit_file: stage/circuit_file
      hoc_dir: emodel_adapt/hoc_dir
      nodes_file: emodel_currents/nodes_file
      output_dir: output_dir
    out:
      - circuit

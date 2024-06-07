cwlVersion: v1.2
class: Workflow

id: neurons_morphology_synthesis
label: Morphology assignment of synthesized morphologies


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
      doc: Circuit bundle with me-types and morphologies.
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
      - morphologies_dir

  - id: stage
    run: ./stage.cwl
    in:
      circuit_id: circuit_id
      stage_dir: setup/stage_dir
      configuration_id: configuration_id
    out:
      - atlas_dir
      - atlas_file
      - nodes_file
      - circuit_file
      - canonical_config_file
      - placeholder_config_file

  - id: transform
    run: transform.cwl
    in:
      atlas_file: stage/atlas_file
      canonical_config_file: stage/canonical_config_file
      transform_dir: setup/transform_dir
    out:
      - region_file
      - parameters_file
      - distributions_file

  - id: split
    run: ./split.cwl
    in:
      nodes_file: stage/nodes_file
      canonical_config_file: stage/canonical_config_file
      output_dir: setup/build_dir
    out:
      - synthesized_nodes_file
      - placeholder_nodes_file

  - id: assign_placeholders
    run: ./assign_placeholders.cwl
    in:
      nodes_file: split/placeholder_nodes_file
      config_file: stage/placeholder_config_file
      out_nodes_file:
        source: setup/build_dir
        valueFrom: $(self.path)/placeholders_with_morphs.h5
      out_morphologies_dir: setup/morphologies_dir
    out:
      - nodes_file

  - id: assign_synthesized
    run: ./assign_synthesized.cwl
    in:
      atlas_dir: stage/atlas_dir
      nodes_file: split/synthesized_nodes_file
      out_nodes_file:
        source: setup/build_dir
        valueFrom: $(self.path)/synthesized_with_morphs.h5
      out_morphologies_dir: setup/morphologies_dir
      region_file: transform/region_file
      parameters_file: transform/parameters_file
      distributions_file: transform/distributions_file
    out:
      - nodes_file

  - id: merge
    run: ./merge.cwl
    in:
      synthesized_nodes_file: assign_synthesized/nodes_file
      placeholder_nodes_file: assign_placeholders/nodes_file
      out_nodes_file:
        source: setup/build_dir
        valueFrom: $(self.path)/nodes.h5
    out:
      - nodes_file

  - id: register
    run: ./register.cwl
    in:
      circuit_id: circuit_id
      nodes_file: merge/nodes_file
      morphologies_dir: setup/morphologies_dir
      output_dir: output_dir
    out:
      - circuit

cwlVersion: v1.2
class: Workflow

id: array-types-workflow
label: Use array types in a workflow


inputs:
  filesA:
    type: File[]
  filesB:
    type: File[]
  one_file:
    type: File

outputs:
  output_file:
    type: File
    outputSource: s1/output_file

steps:

  - id: s0
    run: ./array_types.cwl
    in:
      filesA: filesA
      filesB: filesB
      filesC:
        valueFrom:
          - c_foo
          - c_bar
    out:
      - output_file

  - id: s1
    run: ./array_types.cwl
    in:
      filesA: filesA
      filesB:
        source:
          - s0/output_file
          - one_file
        valueFrom:
          - $(self[0].path)
          - $(self[1].path)
          - e.txt
          - f.txt
      filesC:
        source:
          - s0/output_file
          - one_file
    out:
      - output_file

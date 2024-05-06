cwlVersion: v1.2
class: Workflow

requirements:
  InlineJavascriptRequirement: {}
  StepInputExpressionRequirement: {}

inputs:
  input_file:
    type: File

  output_file:
    type: string

outputs:
  out:
    type: File
    outputSource: s2/out_file

steps:

  s1:
    in:
      source_file: input_file
      target_file:
        source: input_file
        valueFrom: $(self.path)_intm.txt
    out: [out_file]
    run: ./copy.cwl

  s2:
    in:
      source_file: s1/out_file
      target_file: output_file
    out: [out_file]
    run: ./copy.cwl

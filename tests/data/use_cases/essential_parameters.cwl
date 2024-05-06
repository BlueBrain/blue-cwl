#!/usr/bin/env cwl-runner
cwlVersion: v1.2
id: essential-parameters
class: CommandLineTool
baseCommand: echo
inputs:
  example_flag:
    type: boolean
    inputBinding:
      position: 1
      prefix: -f
  example_string:
    type: string
    inputBinding:
      position: 5
      prefix: --example-string
  example_int:
    type: int
    inputBinding:
      position: 2
      prefix: -i
      separate: false
  example_file:
    type: File
    inputBinding:
      prefix: --file=
      separate: false
      position: 4
  example_float:
    type: float
    inputBinding:
      prefix: -d
      position: 3
      separate: false

outputs: []

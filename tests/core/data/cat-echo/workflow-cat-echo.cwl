cwlVersion: v1.2
class: Workflow

id: cat-echo
label: make-some-files

inputs:
  - id: msg0
    type: string

  - id: msg1
    type: string

  - id: msg2
    type: string

outputs:

    output1:
      type: File
      outputSource: c0/cat_out

    output2:
      type: File
      outputSource: c1/cat_out

    output3:
      type: File
      outputSource: d0/cat_out

steps:

  - id: m0
    run: ./echo.cwl
    in:
      message: msg0
    out:
      - example_file

  - id: m1
    run: ./echo.cwl
    in:
      message: msg1
    out:
      - example_file

  - id: m2
    run: ./echo.cwl
    in:
      message: msg2
    out:
      - example_file

  - id: c0
    run: ./cat.cwl
    in:
      f0: m0/example_file
      f1: m1/example_file
    out:
      - cat_out

  - id: c1
    run: ./cat.cwl
    in:
      f0: m1/example_file
      f1: m2/example_file
    out:
      - cat_out

  - id: d0
    run: ./cat.cwl
    in:
      f0: c0/cat_out
      f1: c1/cat_out
    out:
      - cat_out

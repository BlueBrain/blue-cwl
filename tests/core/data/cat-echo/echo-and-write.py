#!/usr/bin/env python3
import sys

with open("file-output.txt", "w") as fd:
    a = " ".join(sys.argv[1:])
    fd.write(a)
    print(a, end="")

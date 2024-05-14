#!/usr/bin/env python

import sys
from pathlib import Path


if __name__ == "__main__":
    Path("file-output.txt").write_text(sys.argv[1])

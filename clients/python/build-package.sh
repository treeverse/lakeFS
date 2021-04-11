#!/bin/sh -e
python -m pip install build --user
python -m build --sdist --wheel --outdir dist/

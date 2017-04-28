#!/bin/bash

set -ex
source bootstrap
pip install -r requirements-docs.txt
python setup.py checkdocs
python setup.py build_sphinx
tox

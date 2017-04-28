#!/bin/bash

set -ex
source bootstrap
python setup.py checkdocs
python setup.py build_sphinx
tox

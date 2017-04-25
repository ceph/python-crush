#!/bin/bash

set -ex
source bootstrap
python setup.py checkdocs
tox

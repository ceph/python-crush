#!/bin/bash
set -e -x

yum install -y gcc gcc-c++ cmake boost148-devel libatomic_ops-devel

: ${PYBINS:=/opt/python/cp27-cp27mu/bin /opt/python/cp3[4-9]-cp3[4-9]m/bin}

for PYBIN in $PYBINS; do
    "${PYBIN}/pip" install -r /io/requirements.txt
    rm -fr /tmp/crush
    git clone /io/ /tmp/crush
    "${PYBIN}/pip" wheel /tmp/crush/ -w wheelhouse/
done

# Bundle external shared libraries into the wheels
mkdir repaired
for whl in wheelhouse/*.whl; do
    if echo $whl | grep --quiet crush ; then
        auditwheel repair "$whl" -w repaired/
    else
        cp $whl repaired/
    fi
done

# Install packages and test
for PYBIN in $PYBINS ; do
    "${PYBIN}/pip" install virtualenv
    rm -fr /tmp/v
    "${PYBIN}/virtualenv" /tmp/v
    /tmp/v/bin/pip install crush --no-index -f repaired/
    /tmp/v/bin/crush --help
done

rm -fr /io/wheelhouse
mkdir /io/wheelhouse
cp repaired/*crush* /io/wheelhouse

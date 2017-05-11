crush
=====

crush is a library to control placement in a hierarchy

- Home page : http://http://libcrush.org/main/python-crush
- Documentation : http://crush.readthedocs.org/
- PyPi : https://pypi.python.org/pypi/crush

GNU/Linux Installation
======================

* pip install crush

Other Installation
==================

When using pip versions lower than 8.1 or other operating systems,
compilation is necessary and packages must be installed first.

* apt-get install -y gcc g++ python-pip python-all-dev libpython3-all-dev cmake libboost-all-dev libatomic-ops-dev
* dnf / yum / zypper install -y gcc gcc-c++ python-pip python-devel python3-devel cmake make boost-devel libatomic_ops-devel
* pip install crush

Hacking
=======

* Get the code:: 

   git clone http://libcrush.org/main/python-crush.git
   cd python-crush

* Set up the development environment::

   deactivate || true ; source bootstrap

* Run the tests::

   deactivate || true ; bash run-tests.sh

* Sync the libcrush submodule::

   git submodule update --remote libcrush

* Run a single test::

   tox -e py27 -- -s -k test_one tests/test_crush.py

* Check the documentation::

   python setup.py build_sphinx
   firefox build/html/index.html

* Update requirements

   rm -fr virtualenv
   virtualenv virtualenv
   source virtualenv/bin/activate
   # update some module in requirements.txt
   tox
   # if that works
   pip install -r requirements.txt
   pip freeze -r requirements.txt > new-requirements.txt
   .tox/py3/bin/pip freeze -r requirements-dev.txt > new-requirements-dev.txt
   diff <(.tox/py27/bin/pip freeze -r requirements-dev.txt) new-requirements-dev.txt
   # all lines after the first "were added by pip freeze" are indirect dependencies
   remove pkg-resources==0.0.0 https://bugs.launchpad.net/ubuntu/+source/python-pip/+bug/1635463

Release management
==================

* Prepare a new version

 - git checkout master ; git pull
 - version=1.0.0 ; perl -pi -e "s/^version.*/version = $version/" setup.cfg ; for i in 1 2 ; do python setup.py sdist ; amend=$(git log -1 --oneline | grep --quiet "version $version" && echo --amend) ; git commit $amend -m "version $version" ChangeLog setup.cfg ; git tag -a -f -m "version $version" $version ; done

* Publish a new version

 - docker build --tag manylinux manylinux
 - docker run --rm -v $(pwd):/io manylinux /io/manylinux/build-wheels.sh
   OR docker run --rm -v $(pwd):/io manylinux env PYBINS=/opt/python/cp27-cp27mu/bin /io/manylinux/build-wheels.sh
 - sudo chown -R $(id -u) wheelhouse/
 - twine upload --sign wheelhouse/*crush*

 - rm -fr dist
 - python setup.py sdist
 - twine upload --sign dist/\*.tar.gz

 - git push ; git push --tags

* pypi maintenance

 - trim old versions at https://pypi.python.org/pypi/crush

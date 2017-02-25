crush
=====

crush is a library to control placement in a hierarchy

- Home page : http://http://libcrush.org/main/python-crush
- Documentation : http://crush.readthedocs.org/
- PyPi : https://pypi.python.org/pypi/crush

Installation
============

* apt-get install -y gcc g++ python-all-dev libpython3-all-dev cmake
* dnf / yum / zypper install -y gcc gcc-c++ python-devel python3-devel cmake
* pip install crush

Hacking
=======

* Get the code:: 

   git clone --recursive http://libcrush.org/main/python-crush.git

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

Release management
==================

* Prepare a new version

 - version=1.3.0 ; perl -pi -e "s/^version.*/version = $version/" setup.cfg ; for i in 1 2 ; do python setup.py sdist ; amend=$(git log -1 --oneline | grep --quiet "version $version" && echo --amend) ; git commit $amend -m "version $version" ChangeLog setup.cfg ; git tag -a -f -m "version $version" $version ; done

* Publish a new version

 - python setup.py sdist upload --sign
 - git push ; git push --tags

* pypi maintenance

 - trim old versions at https://pypi.python.org/pypi/crush

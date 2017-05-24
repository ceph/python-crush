Hacking
=======

Synchronisation with libcrush
-----------------------------

The `crush/libcrush` directory tracks master from
http://github.com/ceph/ceph and should be updated to get the latest
changes with::

    git clone http://github.com/ceph/ceph /tmp/ceph
    git revert 8f07936 # commit with change to make crush standalone
    cd crush/libcrush
    for file in {common,crush,include}/*.[ch] {common,crush}/*.cc ; do
       cp /tmp/ceph/src/$file $file
    done
    git commit -m 'libcrush: sync with Ceph'
    git cherry-pick 8f07936
    # update the hash above with the new hash
    git commit -m 'doc: reference to the last sync with Ceph'

The commit message should then be updated with the hash of the
ceph repository from which the files were copied.

Part of the sources it contains are compiled in the crush.libcrush
module, as instructed in the **[extension-crush.libcrush]** section of
the `setup.cfg` file:

.. literalinclude:: ../../setup.cfg
   :start-after: [extension-crush.libcrush]
   :end-before: [build_ext]
   :language: ini

The acconfig.h file is created by `cmake` via the build hooks
mentionned in the `[build_ext]` section of the `setup.cfg` file:

.. literalinclude:: ../../setup.cfg
   :start-after: [build_ext]
   :language: ini

The definition of the functions are in the `setup/_setup_hooks.py` file:

.. literalinclude:: ../../setup/_setup_hooks.py
   :language: python

Debuging
--------

The `libcrush` CPython module can be debugged by running tests from
`test/test_libcrush.py` in verbose mode with commands such as::

    tox -e py27 -- -vv -s -k test_parse_invalid_type tests/test_libcrush.py

For some reason py3 does not flush stdout and should be avoided. It is quite useful
to see the output up to the point where a core dump happens.

Increasing libcrush verbosity
-----------------------------

The `cursh/libcrush` module files must be recompiled to display a
verbose output when they show lines like::

    #define dprintk(args...) /* printf(args) */

Testing installation on RPM OS
------------------------------

The instructions are duplicated in:

- docs/index.rst
- README.rst
- boostrap

Start by updating the `bootstrap` instructions and try it manually with::

    cd ~/software/libcrush/libcrush
    sudo docker run -ti -v $(pwd):$(pwd) -w $(pwd) opensuse:42.2 bash
    zypper install -y which
    source bootstrap

Debugging readthedocs
---------------------

- https://readthedocs.org/dashboard/crush/versions/
- click "Active" for the desired branch from http://libcrush.org/main/python-crush/
- https://readthedocs.org/projects/crush/builds/
- explicitly ask a build for the designated branch, does not happen automatically
- in https://readthedocs.org/projects/crush/builds/\*/ check all lines from the bottom
  some may contain error messages that are do not fail the build but do nothing useful
  either


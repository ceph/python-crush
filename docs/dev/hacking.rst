Hacking
=======

Synchronisation with libcrush
-----------------------------

The `libcrush` submodule tracks master from
http://libcrush.org/main/libcrush and should be updated to get the latest changes with::

    git submodule update --remote libcrush

Part of the sources it contains are copied and compiled in the
crush.libcrush module, as instructed in the
**[extension-crush.libcrush]** section of the `setup.cfg` file:

.. literalinclude:: ../../setup.cfg
   :start-after: [extension-crush.libcrush]
   :end-before: [build_ext]
   :language: ini

The `libcrush_cmake` and `libcrush_headers` are interpreted by the
build hooks mentionned in the `[build_ext]` section of the `setup.cfg` file:

.. literalinclude:: ../../setup.cfg
   :start-after: [build_ext]
   :language: ini

The definition of the functions are in the `crush/_setup_hooks.py` file:

.. literalinclude:: ../../crush/_setup_hooks.py
   :language: python

Since the files are copied from the `libcrush` submodule before each build, the
compilation happens even if there was no change in the sources.

After a file is removed or added from the `libcrush` submodule, the
relevant section of `setup.cfg` must be updated as well as the
`MANIFEST.in` file which must contain the list of all files copied
from the `libcrush` submodule.

.. literalinclude:: ../../MANIFEST.in

The `MANIFEST.in` file is used when creating packages with::

    python setup.py sdist

Debuging
--------

The `libcrush` CPython module can be debugged by running tests from
`test/test_libcrush.py` in verbose mode with commands such as::

    tox -e py27 -- -s -k test_parse_invalid_type tests/test_libcrush.py

For some reason py3 does not flush stdout and should be avoided. It is quite useful
to see the output up to the point where a core dump happens.

Increasing libcrush verbosity
-----------------------------

The `libcrush` submodule files must be recompiled to display a verbose output::

    #define dprintk(args...) /* printf(args) */

It is enough to uncomment in the submodule files since they are always copied
before building the extension.

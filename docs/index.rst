crush
=====

`crush
<http://http://libcrush.org/main/python-crush>`_ is a
:ref:`GPLv3+ Licensed <gplv3>` library to control placement in a hierarchy.

Installation
------------

* apt-get install -y gcc g++ python-all-dev libpython3-all-dev cmake
* dnf / yum / zypper install -y gcc gcc-c++ python-devel python3-devel cmake
* pip install crush

Quick start
-----------

Mapping the object 1234 to two devices in different hosts:

.. literalinclude:: quick.py
   :language: python

Output::

    [u'device1']
    [u'device1', u'device5']

API
---

.. toctree::
   :maxdepth: 1

   api

Contributor Guide
-----------------

If you want to contribute to ``crush``, this part of the documentation is for
you.

.. toctree::
   :maxdepth: 1

   dev/authors

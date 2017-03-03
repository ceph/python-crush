crush
=====

`crush <http://libcrush.org/main/python-crush>`_ is a `GPLv3+
Licensed <https://www.wikidata.org/wiki/Q27016754>`_ library to control placement in a hierarchy.

Introduction
------------

The `crush` module wraps the `libcrush
<http://http://libcrush.org/main/python-crush>`_ C library
implementing `CRUSH
<http://www.crss.ucsc.edu/media/papers/weil-sc06.pdf>`_, a scalable
pseudo-random data distribution function designed for distributed
object storage systems that efficiently maps data objects to storage
devices without relying on a central directory.

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

   dev/hacking
   dev/authors

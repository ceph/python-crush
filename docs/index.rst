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

API quick start
---------------

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

CLI quick start
---------------

How many objects will move after adding two new devices in the
crushmap ?
::

    $ crush compare --rule firstn \
                    --replication-count 1 \
                    --origin before.json --destination after.json
    There are 1000 objects.

    Replacing the crushmap specified with --origin with the crushmap
    specified with --destination will move 229 objects (22.9% of the total)
    from one item to another.

    The rows below show the number of objects moved from the given
    item to each item named in the columns. The objects% at the end of
    the rows shows the percentage of the total number of objects that
    is moved away from this particular item. The last row shows the
    percentage of the total number of objects that is moved to the
    item named in the column.

             osd.8    osd.9    objects%
    osd.0        3        4       0.70%
    osd.1        1        3       0.40%
    osd.2       16       16       3.20%
    osd.3       19       21       4.00%
    osd.4       17       18       3.50%
    osd.5       18       23       4.10%
    osd.6       14       23       3.70%
    osd.7       14       19       3.30%
    objects%   10.20%   12.70%   22.90%

Given a Ceph crushmap, show which hosts will be overused or underused::

    $ ceph osd crush dump > crushmap-ceph.json
    $ crush ceph --convert crushmap-ceph.json > crushmap.json
    $ crush analyze --rule replicated --crushmap crushmap.json

Output::

            ~id~  ~weight~  ~over/under used %~
    ~name~
    g9       -22  2.299988     10.40
    g3        -4  1.500000     10.12
    g12      -28  4.000000      4.57
    g10      -24  4.980988      1.95
    g2        -3  5.199982      1.90
    n7        -9  5.484985      1.25
    g1        -2  5.880997      0.50
    g11      -25  6.225967     -0.95
    g8       -20  6.679993     -1.73
    g5       -15  8.799988     -7.88

CLI
---

The `crush` command has a set of subcommands to manipulate and analyze
crushmaps. Each subcommand is fully documented with `crush subcommand -h`::

    $ crush --help
    usage: crush [-h] [-v] {analyze,ceph} ...

    A library to control placement in a hierarchy

    optional arguments:
      -h, --help      show this help message and exit
      -v, --verbose   be more verbose

    subcommands:
      valid subcommands

    {analyze,compare,ceph} sub-command -h

        analyze       Analyze crushmaps
        compare       Compare crushmaps
        ceph          Ceph support

Contributor Guide
-----------------

If you want to contribute to ``crush``, this part of the documentation is for
you.

.. toctree::
   :maxdepth: 1

   dev/hacking
   dev/authors

crush
=====

`python-crush <http://libcrush.org/main/python-crush>`_ is a `GPLv3+
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

GNU/Linux Installation
----------------------

* pip install crush

Other Installation
------------------

When using pip versions lower than 8.1 or other operating systems,
compilation is necessary and packages must be installed first.

* apt-get install -y gcc g++ python-pip python-all-dev libpython3-all-dev cmake libboost-all-dev libatomic-ops-dev
* dnf / yum / zypper install -y gcc gcc-c++ python-pip python-devel python3-devel make cmake boost-devel libatomic_ops-devel
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

Given a Ceph crushmap, show which hosts will be overfilled or underfilled::

    $ ceph osd crush dump > crushmap-ceph.json
    $ crush analyze --rule replicated --crushmap crushmap-ceph.json

            ~id~  ~weight~  ~objects~  ~over/under filled %~
    ~name~
    host2     -4       1.0         70                    5.0
    host0     -2       1.0         65                   -2.5
    host1     -3       1.0         65                   -2.5

Rebalance a Ceph pool::

    $ ceph report > report.json
    $ crush optimize --crushmap report.json --out-path optimized.crush --pool 3
    default optimizing
    default wants to swap 10 objects
    default will swap 10 objects
    cloud3-1359 optimizing
    cloud3-1360 optimizing
    ...
    $ ceph osd setcrushmap -i optimized.crush

CLI
---

The `crush` command has a set of subcommands to manipulate and analyze
crushmaps. Each subcommand is fully documented with `crush subcommand -h`::

    $ crush --help
    usage: crush [-h] [-v] [--debug] [--no-backward-compatibility] {analyze,compare,optimize,convert} ...

    Ceph crush compare and analyze

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         be more verbose
      --debug               debugging output, very verbose
      --no-backward-compatibility
                            do not allow backward compatibility tunables (default: allowed)

    subcommands:
      valid subcommands

      {analyze,compare,optimize,convert}
                            sub-command -h
        analyze             Analyze crushmaps
        compare             Compare crushmaps
        optimize            Optimize crushmaps
        convert             Convert crushmaps

Cookbook
--------

.. toctree::
   :maxdepth: 1

   ceph/optimize


Contributor Guide
-----------------

If you want to contribute to ``crush``, this part of the documentation is for
you.

.. toctree::
   :maxdepth: 1

   dev/hacking
   dev/authors

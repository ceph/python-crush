# -*- mode: python; coding: utf-8 -*-
#
# Copyright (C) 2017 <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
try:  # chicken and egg problem when running pip install -e . from sources
    from crush.libcrush import LibCrush
except:
    pass


class Crush(object):
    """Control object placement in a hierarchy.

    The algorithms optimize the placement so that:

    - the devices are filled according to their weight

    - the number of objects that move is proportional to the magnitude
      of the weight of the devices removed or added

    """

    def __init__(self, verbose=False):
        """Create a Crush.

        If the optional argument `verbose` is set to True, all methods
        will print debug information on stdout. It defaults to False.

        """
        self.c = LibCrush(verbose and 1 or 0)

    def parse(self, crushmap):
        """Validate and parse the `crushmap` object.

        The `crushmap` is a hierarchical description of devices in
        which objects can be stored and rules to place the objects.
        It is verified to obey the specifications below. An exception
        is raised on the first error found.
        ::

            crushmap = {
              # optional (default: none)
              "trees": trees,

              # optional (default: none)
              "rules": rules,
            }

        The "trees" are the roots of device hierarchies, the "rules" describe
        various object placement strategies for this device hierarchy.
        ::

            trees = {
              # optional (default: none)
              <root name str>: bucket or device,
              <root name str>: bucket or device,
              ...
            }

        The **root name** is the name of a top level bucket or device.
        ::

            bucket = {
              # mandatory
              "~type~": <str>,

              # optional (default: first available id)
              "~id~": <negative int>,

              # optional (default: "straw2")
              "~algorithm~": "uniform" or "list" or "straw2",

              # optional (default: cumulated children weights or none)
              "~weight~": <postive float>,

              # optional (default: none)
              <child name str>: bucket or device,
              <child name str>: bucket or device,
              ...
            }

        The **~type~** is a user defined string that can be used by
        **rules** to select all buckets of the same type.

        The **~id~** must either be set for all buckets or not at
        all. If the **~id~** is provided, it must be a unique negative
        number. If it is not provided, the first available id is
        used.

        The **~weight~** must either be set for all buckets or not at
        all. If not set, **~weight~** defaults to the cumulated weight
        of the immediate children bucket or devices, recursively,
        bottom to top.

        Children within a bucket are chosen with one of three
        **~algorithms~** representing a tradeoff between performance
        and reorganization efficiency. If you are unsure, we recommend
        using **"straw2"**. The table summarizes how the speed of each
        option measures up against mapping stability when items are
        added or removed::

            Bucket Alg     Speed       Additions    Removals
            ------------------------------------------------
            "straw2"        O(n)       optimal      optimal
            "uniform"       O(1)       poor         poor
            "list"          O(n)       optimal      poor

        - **"straw2"**: List and tree buckets are structured such that
          a limited number of hash values need to be calculated and
          compared to weights in order to select a bucket child. In
          doing so, they divide and conquer in a way that either gives
          certain children precedence (e. g., those at the beginning
          of a list) or obviates the need to consider entire subtrees
          of children at all. That improves the performance of the
          replica placement process, but can also introduce suboptimal
          reorganization behavior when the contents of a bucket change
          due an addition, removal, or re-weighting of an child. The
          straw2 bucket type allows all children to fairly `compete`
          against each other for replica placement through a process
          analogous to a draw of straws. To place a replica, a straw
          of random length is drawn for each child in the bucket.  The
          child with the longest straw wins.  The length of each straw
          is initially a value in a fixed range.  Each straw length is
          scaled by a factor based on the child’s weight so that
          heavily weighted children are more likely to win the draw.
          Although this process is almost twice as slow (on average)
          than a list bucket and even slower than a tree bucket (which
          scales logarithmically), straw2 buckets result in optimal
          data movement between nested children when modified.

        - **"uniform"**: Devices are rarely added individually in a
          large system.  Instead, new storage is typically deployed in
          blocks of identical devices, often as an additional shelf in
          a server rack or perhaps an entire cabinet. Devices reaching
          their end of life are often similarly decommissioned as a
          set (individual failures aside), making it natural to treat
          them as a unit. Uniform buckets are used to represent an
          identical set of devices in such circumstances. The key
          advantage in doing so is performance related: Crush can map
          replicas into uniform buckets in constant time. In cases
          where the uniformity restrictions are not appropriate, other
          bucket types can be used. If the size of a uniform bucket
          changes, there is a complete reshuffling of data between
          devices, much like conventional hash-based distribution
          strategies.

        - **"list"**: List buckets structure their contents as a
          linked list, and can contain children with arbitrary
          weights.  To place a replica, Crush begins at the head of
          the list with the most recently added child and compares its
          weight to the sum of all remaining children’ weights.
          Depending on the value of the hash function, either the
          current child is chosen with the appropriate probability, or
          the process continues recursively down the list. This is a
          natural and intuitive choice for an expanding cluster:
          either an object is relocated to the newest device with some
          appropriate probability, or it remains on the older devices
          as before.  The result is optimal data migration when
          children are added to the bucket. Children removed from the
          middle or tail of the list, however, can result in a
          significant amount of unnecessary movement, making list
          buckets most suitable for circumstances in which they never
          (or very rarely) shrink.

        There can be many children or none.
        ::

            device = {
              # mandatory
              "~id~": <positive int>,

              # optional (default: 1.0)
              "~weight~": <postive float>,
            }

        The **~id~** must be a unique positive number.

        If the **~weight~** of a device A is lower than the
        **~weight~** of a device B, it will be less likely to be used.
        A common pattern is to set the **~weight~** to 2.0 for 2TB
        devices, 1.0 for 1TB devices, 0.5 for 500GB devices, etc.
        ::

            rules = {
              # optional (default: none)
              <rule name str>: rule,
              <rule name str>: rule,
              ...
            }

        A **rule** maps an object (see the map function) to a list
        of devices. There can be multiple rules depending on the
        mapping strategy.
        ::

            rule = [ step, step, ... ]

        The **rule** interprets each **step** in sequence and the last
        one must be **"emit"**.
        ::

            step = [ "take", <bucket name str> ]

        Select the **bucket name**.
        ::

            step = [
              "choose_firstn" or "choose_indep",
              <replication count positive int>,
              <bucket type str>
            ]

        Recursively explore each bucket currently selected, looking for
        **replication count** buckets of the required **bucket type**
        and select them.

        If **replication count** is zero, the number of buckets
        to select will be determined by the `replication_count` argument of
        the `map` method, i.e. **replication count** is set to
        match the desired number of replicas.
        ::

            step = [
              "chooseleaf_firstn" or "chooseleaf_indep",
              <replication count positive int>,
              <bucket type str>
            ]

        Recursively explore each bucket currently selected, looking for
        **replication count** devices within all buckets of
        the required **bucket type** and select them.

        If **replication count** is zero, the number of devices
        to select will be determined by the `replication_count` argument of
        the `map` method, i.e. **replication count** is set to
        match the desired number of replicas.
        ::

            step = [ "emit" ]

        Append the selection to the results and clear the selection.
        """
        self.c.parse(crushmap)
        return True

    def map(self, rule, value, replication_count, weights=None):
        """Map an object to a list of devices.

        The **rule** is used to map the **value** (representing an
        object) to the desired number of devices
        (**replication_count**) and return them in a list. The
        probabilities for a given device to be selected can be
        modified by the **weights** dictionnary.

        If the mapping is successful, a list of device containing
        exactly **replication_count** devices is returned. If the
        mapping fails, the list may contains less devices or some
        names may be replaced by None. For instance, if asking
        for 3 replicas, the result of a failed mapping may be::

            [ "device1", "device5" ] # 2 instead of 3
            [ "device8", None, "device0" ] # second device is missing

        The dictionary of **weights** modifies the probabilities for
        the device names it contains. The value is a float in the
        range [0..1]. If the weight is 0, the device will never be
        selected.  If the weight is 1, the probability that the device
        is selected is not modified. If a device is not in the
        **weights** dictionary, its probability is not modified. For
        example::

            { "device0": 0.50, "device1": 0.75 }

        will multiply the probability of "device0" by 0.50 (i.e,
        reduce it by 50%) and "device1" by 0.75 (i.e, reduce it by
        25%).

        - **rule**: the rule name (required string)

        - **value**: the number to map (required integer)

        - **replication_count**: the desired number of devices
            (required positive integer)

        - **weights**: map of name to weight float (optional, default to None)

        Return a list of device names.

        """
        kwargs = {
            "rule": rule,
            "value": value,
            "replication_count": replication_count,
        }
        if weights:
            kwargs["weight"] = weights
        return self.c.map(**kwargs)

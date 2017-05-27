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
from __future__ import division

import collections
import copy
import json
import logging
from crush.libcrush import LibCrush

log = logging.getLogger(__name__)


class Crush(object):
    """Control object placement in a hierarchy.

    The algorithms optimize the placement so that:

    - the devices are filled according to their weight

    - the number of objects that move is proportional to the magnitude
      of the weight of the devices removed or added

    **Choose and chooseleaf retries**

    The "choose firstn" and "choose indep" rule step look for buckets
    of a given type, randomly selecting them. If they are unlucky and
    find the same bucket twice, they will try N+1 times (N being the
    value of the choose_total_tries tunable). If there is a previous
    "set_choose_tries" step in the same rule, it will try O times
    instead (O being the value of the argument of the
    "set_choose_tries" step).

    Note: the choose_total_tries tunable is the number of retry, not
    the number of tries. The number of tries is the number of retry +
    1. The "set_choose_tries" rule step sets the number of tries and
    does not need the + 1. This confusing difference is inherited from
    an off-by-one bug from years ago.

    The "chooseleaf firstn" and "chooseleaf indep" rule step do the same
    as "choose firstn" and "choose indep" but also recursively explore
    each bucket found, looking for a single device. The same device
    may be found in two different buckets because the crush map is not
    a strict hierarchy, it is a DAG. When such a collision happens,
    they will try again. The number of times they try to find a non
    colliding device is:

    - If "firstn" and there is no previous "set_chooseleaf_tries" rule step: try
      N + 1 times (N being the value of the choose_total_tries tunable)

    - If "firstn" and there is a previous "set_chooseleaf_tries" rule
      step: try P times (P being the value of the argument of the
      "set_chooseleaf_tries" rule step)

    - If "indep" and there is no previous "set_chooseleaf_tries" rule step: try
      1 time.

    - If "indep" and there is a previous "set_chooseleaf_tries" rule step:
      try P times (P being the value of the argument of the
      "set_chooseleaf_tries" rule step)

    **Backward compatibility**

    Some tunables and rule steps only exist for backward
    compatibility. Trying to use them will raise an exception, unless
    Crush is created with backward_compatibility=True. They are listed
    here for reference but they are documented in the libcrush api
    at http://doc.libcrush.org/master/group___a_p_i.html.

    In the crushmap::

            tunables = {
              "choose_local_tries": 0,
              "choose_local_fallback_tries": 0,
              "chooseleaf_vary_r": 1,
              "chooseleaf_stable": 1,
              "chooseleaf_descend_once": 1,
              "straw_calc_version": 1,
            }

    In a rule step::

            [ "set_choose_local_tries", <positive integer> ]
            [ "set_choose_local_fallback_tries", <positive integer> ]
            [ "set_chooseleaf_vary_r", 0 or 1 ]
            [ "set_chooseleaf_stable", 0 or 1 ]

    """

    def __init__(self, verbose=False, backward_compatibility=False):
        """Create a Crush.

        If the optional argument `verbose` is set to True, all methods
        will print debug information on stdout. It defaults to False.

        If the optional argument `backward_compatibility` is set to
        True, the tunables designed for backward_compatibility are
        allowed, otherwise trying to use them raises an exception. See
        **Backward compatibility** in the class documentation.

        """
        self.c = LibCrush(verbose=verbose and 1 or 0,
                          backward_compatibility=backward_compatibility and 1 or 0)

    def parse(self, something):
        """Validate and parse `something` which can be one of the
        following:

        - a crushmap dict as documented in the parse_crushmap() method

        - a path to a file containing a JSON representation of a
          crushmap as documented in the Crush.parse_crushmap() method

        - a path to a file containing a Ceph binary, text or JSON
          crushmap compatible with Luminuous and below

        The details of the validation and parsing are documented
        in the parse_crushmap() method.

        """
        return self.parse_crushmap(self._convert_to_crushmap(something))

    def parse_crushmap(self, crushmap):
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

              # optional (default: none)
              "choose_args": choose_arg_map,

              # optional (default: none)
              "tunables": tunables,
            }

        The "trees" are the roots of device hierarchies, the "rules" describe
        various object placement strategies for this device hierarchy.
        ::

            trees = [
              # optional (default: none)
              bucket or device or reference,
              bucket or device or reference,
              ...
            ]

        Each element can either be a device (i.e. no children and id >= 0),
        a bucket (i.e. id < 0) or a reference to a bucket or device.
        ::

            bucket = {
              # mandatory
              "type": <str>,

              # mandatory
              "name": <str>,

              # optional (default: first available id)
              "id": <negative int>,

              # optional (default: "straw2")
              "algorithm": "uniform" or "list" or "straw2",

              # optional (default: cumulated children weights or none)
              "weight": <postive Q16.16>,

              # optional (default: none)
              "children": children
            }

        The **type** is a user defined string that can be used by
        **rules** to select all buckets of the same type.

        The **name** is a user defined string that uniquely identifies
        the bucket.

        The **id** must either be set for all buckets or not at
        all. If the **id** is provided, it must be a unique negative
        number. If it is not provided, the first available id is
        used.

        The **weight** must either be set for all buckets or not at
        all. If not set, **weight** defaults to the cumulated weight
        of the immediate children bucket or devices or reference,
        recursively, bottom to top.

        Children within a bucket are chosen with one of three
        **algorithms** representing a tradeoff between performance
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

        There also exists an older version of "straw2", known as **"straw"**.
        Given the choice, it is *always* better to use "straw2"; "straw" is
        therefore not allowed by default. If the crushmap being read is old
        and still uses "straw", setting `backward_compatibility=True` when
        creating the `Crush` object will allow it to be used.
        ::

            children = [
              # optional (default: none)
              bucket or device or reference,
              bucket or device or reference,
              ...
            ]

        Each element can either be a device (i.e. no children and id >= 0),
        a bucket (i.e. id < 0) or a reference to a bucket or device.
        ::

            reference = {
              # mandatory
              "reference_id": <int>,

              # optional (default: 1.0)
              "weight": <postive Q16.16>,
            }

        The **reference_id** must be equal to the **id** of a bucket or
        device defined in the hierarchy.

        If the **weight** is omitted, it default to 0x10000. The
        **weight** must either be set for all references or not at
        all.
        ::

            device = {
              # mandatory
              "id": <positive int>,

              # mandatory
              "name": <str>,

              # optional (default: 0x10000)
              "weight": <postive Q16.16>,
            }

        The **id** must be a unique positive number.

        The **name** is a user defined string that uniquely identifies
        the device.

        If the **weight** of a device A is lower than the
        **weight** of a device B, it will be less likely to be used.
        A common pattern is to set the **weight** to 0x20000 for 2TB
        devices, 0x10000 for 1TB devices, 0x08000 for 500GB devices, etc.
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
              "set_choose_tries" or "set_chooseleaf_tries",
              <positive integer>
            ]

        Overrides the default number of tries (set by the
        choose_total_tries tunables and defaulting to 50) when looking
        for a bucket (set_choose_tries) or a device
        (set_chooseleaf_tries). See **Choose and chooseleaf retries**
        in the class documentation for more information.
        ::

            step = [
              "choose"
              "firstn" or "indep",
              <replication count positive int>,
              "type"
              bucket type or device type
            ]

        Recursively explore each bucket currently selected, looking for
        **replication count** buckets of the required **bucket type**
        and select them.

        **firstn** is best suited when the order of the result does not matter,
        for instance to place object replicas. **indep** is best suited when
        the order of the result makes a difference, for instance to place parts
        of an erasure coded object.

        If **replication count** is zero, the number of buckets
        to select will be determined by the `replication_count` argument of
        the `map` method, i.e. **replication count** is set to
        match the desired number of replicas.
        ::

            bucket type = <str>

        The type field of a bucket definition is used in rule steps to
        designate all buckets with the same type.
        ::

            device type = 0

        The devices all have the same fixed type: 0.
        ::

            step = [
              "chooseleaf"
              "firstn" or "indep",
              <replication count positive int>,
              "type"
              <bucket type str>
            ]

        Recursively explore each bucket currently selected, looking for
        **replication count** devices within all buckets of
        the required **bucket type** and select them.

        **firstn** is best suited when the order of the result does not matter,
        for instance to place object replicas. **indep** is best suited when
        the order of the result makes a difference, for instance to place parts
        of an erasure coded object.

        If **replication count** is zero, the number of devices
        to select will be determined by the `replication_count` argument of
        the `map` method, i.e. **replication count** is set to
        match the desired number of replicas.
        ::

            step = [ "emit" ]

        Append the selection to the results and clear the selection
        ::

            choose_arg_map = {
              <name str>: choose_args,
              <name str>: choose_args,
              ...
            }

        A named collection of **choose_args** for **map()** to use for
        changing the weights of straw2 buckets or remap the items they
        contains to arbitrary values. See **map()** for more information.
        ::

            choose_args = [
              choose_args_bucket,
              choose_args_bucket,
              ...
            ]

        A list of **choose_args_bucket**, each modifying the weights
        or the ids of a **bucket**. There must be at most one
        **choose_args_bucket** for a given bucket in the crushmap.
        ::

            choose_args_bucket = {
              # mandatory
              "bucket_id": <negative int> or "bucket_name": <bucket name str>,
              # optional (default: none)
              "ids": choose_args_ids,
              # optional (default: none)
              "weight_set": choose_args_weight_set,
            }

        When calling **map()** with this **choose_args_bucket**,
        straw2 will use **choose_args_ids** instead of the items it
        contains. And instead of using the weights stored in the
        bucket, it will use one of **choose_args_weight_set**,
        depending on the position. See **map()** for more information.

        The bucket is uniquely identified by either **bucket_id**
        which must be a negative number or **bucket_name** which is
        the name of the bucket set in the **bucket** definition. The
        two are mutually exclusive.
        ::

            choose_args_ids = [ <int>, <int>, ... ]

        The first element of **choose_args_ids** will be used instead
        of the id of the first children and so on.

        The size of **choose_args_ids** must be exactly the same as
        the size of the **children** array of the corresponding
        **bucket**.
        ::

            choose_args_weight_set = [
               [ <positive Q16.16>, <positive Q16.16>, ... ], # position 0
               [ <positive Q16.16>, <positive Q16.16>, ... ], # position 1
               ...
            ]

        When **map()** chooses the frist replicas from the
        corresponding **bucket** it will use the weights at position 0
        instead of the weights stored in the **bucket**. When choosing
        the second replica it will use the weights at positon 1 and so
        on.

        The size of the array for each position must be exactly the
        same as the size of the **children** array of the corresponding
        **bucket**.

        If **choose_args_weight_set** does not contain a list of weights
        for a given position, the weights in the last available position
        will be used instead.
        ::

            tunables = {
              "choose_total_tries": 50
            }

        A tunable changes the behavior of the **map()** method and it
        will not return the same list of devices if it is changed.

        - **choose_total_tries** (optional positive integer, default:
          50) is the default number of retries on a collision.
          See **Choose and chooseleaf retries** in the class
          documentation for more information.

        """
        self.crushmap = copy.deepcopy(crushmap)
        self.c.parse(self.crushmap)
        self._update_info()
        return True

    def map(self, rule, value, replication_count, weights=None, choose_args=None):
        """Map an object to a list of devices.

        The **rule** is used to map the **value** (representing an
        object) to the desired number of devices
        (**replication_count**) and return them in a list. The
        probabilities for a given device to be selected can be
        modified by the **weights** dictionnary or by **choose_args**
        for straw2 buckets.

        If the mapping is successful, a list of device containing
        exactly **replication_count** devices is returned. If the
        mapping fails, the list may contains less devices or some
        names may be replaced by None. For instance, if asking
        for 3 replicas, the result of a failed mapping could be::

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

        If the **choose_args** is a string, the corresponding list
        will be retrieved from **choose_arg_map** in the map. Each
        element in the **choose_args** list modifies the parameters of
        the choose function for the corresponding straw2 bucket. With
        a bucket containing two children::

            {
               "name": "bucket1":
               "children": [
                   { "name": "device1", "id": 1, "weight": 1 * 0x10000 },
                   { "name": "device2", "id": 2, "weight": 2 * 0x10000 },
               ]
            }

        If map(85) == [ 'device2' ], the result could be different
        by switching the weights with::

            map(85, choose_args=[
              { "bucket_name": "bucket1",
                "weight_set": [ [ 2 * 0x10000, 1 * 0x10000 ] ]
              }]) == [ 'device1' ]

        Similarly the result can be influenced by providing
        alternative ids. The id of each item is a parameter of the
        hash considered for placement. However, contrary to the
        weight, there is no way to guess how it will influence the
        result.::

            map(85, choose_args=[
              { "bucket_name": "bucket1",
                "ids": [ 100, 300 ]
              }]) == [ 'device1' ]

        - **rule**: the rule name (required string)

        - **value**: the number to map (required integer)

        - **replication_count**: the desired number of devices
            (required positive integer)

        - **weights**: map of name to weight float (optional, default to None)

        - **choose_args**: name to lookup in the map or a list (optional, default to None)

        Return a list of device names.

        """
        kwargs = {
            "rule": rule,
            "value": value,
            "replication_count": replication_count,
        }
        if weights:
            kwargs["weights"] = weights
        if choose_args:
            kwargs["choose_args"] = choose_args
        return self.c.map(**kwargs)

    def _convert_to_crushmap(self, something):
        if type(something) in (dict, collections.OrderedDict):
            return something
        else:
            with open(something) as f_json:
                return json.load(f_json, object_pairs_hook=collections.OrderedDict)

    def to_file(self, out_path):
        open(out_path, "w").write(json.dumps(self.crushmap, indent=4, sort_keys=True))

    @staticmethod
    def parse_osdmap_weights(osdmap):
        weights = {}
        for osd in osdmap["osds"]:
            if osd["weight"] < 1.0:
                key = "osd.{}".format(osd["osd"])
                weights[key] = osd["weight"]
        return weights

    @staticmethod
    def parse_weights_file(weights_file):
        """
        Parse a file containing information about devices weights.
        The file is expected to contain JSON data. It can either be:

         - a dictionary that maps weights (as floats between 0 and 1)
           to device names, e.g.::

            {"device0": 0.71, "device1": 0.212}

         - a JSON dump of a Ceph OSDMap, obtainable with the command::

            ceph osd dump --format json > osdmap.json

        :param weights_file: File object to the weights file
        :type weights_file: file
        :return: Parsed weights
        :rtype: dict[str, float]
        """

        f_name = weights_file.name

        # Load the JSON data
        try:
            raw_data = json.load(weights_file)
        except ValueError:
            raise AssertionError("{} is not valid JSON".format(f_name))

        # It should be an object (dict) and not an array (list)
        assert type(raw_data) is dict, "Expected {} to be a dict".format(f_name)

        osdmap_keys = {"epoch", "fsid", "osds", "pools"}  # And many more!
        if all(k in raw_data for k in osdmap_keys):
            weights = Crush.parse_osdmap_weights(raw_data)
        else:
            weights = raw_data

        # Check the weight values
        assert all(type(v) is float and 0 <= v <= 1 for v in weights.values()), \
            "Weights must be floating-point values between 0 and 1"

        # Don't check that the keys are existing devices, LibCrush will do it
        # No need to check that keys are strings, it's enforced by JSON

        return weights

    #
    # Working with the crushmap in memory structure
    #
    def get_crushmap(self):
        """
        Return the original crushmap given to the parse() method.

        The returned crushmap does not contain any reference_id,
        they are replaced by a pointer to the actual bucket. This
        is convenient when exploring the crushmap. But it will
        fail to parse again because duplicated buckets will be
        found.
        """
        return self.crushmap

    def _collect_items(self, children):
        for child in children:
            if 'id' in child:
                self._name2item[child['name']] = child
                self._id2item[child['id']] = child
            self._collect_items(child.get('children', []))

    def _update_info(self):
        self._name2item = {}
        self._id2item = {}
        trees = self.crushmap.get('trees', [])
        self._collect_items(trees)

    def get_item_by_id(self, id):
        return self._id2item[id]

    def get_item_by_name(self, name):
        return self._name2item[name]

    def rule_get_take_failure_domain(self, name):
        rule = self.crushmap['rules'][name]
        take = None
        failure_domain = None
        for step in rule:
            if step[0] == 'take':
                assert take is None
                take = step[1]
            elif step[0].startswith('choose'):
                assert failure_domain is None
                (op, firstn_or_indep, num, _, failure_domain) = step
        return (take, failure_domain)

    def find_bucket(self, name):
        def walk(children):
            for child in children:
                if child.get('name') == name:
                    return child
                found = walk(child.get('children', []))
                if found:
                    return found
            return None
        return walk(self.crushmap.get('trees', []))

    @staticmethod
    def collect_buckets_by_type(root, type):
        def walk(children):
            found = []
            for child in children:
                if child.get('type') == type:
                    found.append(child)
                found.extend(Crush.collect_buckets_by_type(child.get('children', []), type))
            return found
        return walk(root)

    def _merge_choose_args(self):
        if 'choose_args' not in self.crushmap:
            return False
        id2choose_args = collections.defaultdict(lambda: {})
        for name, choose_args in self.crushmap['choose_args'].items():
            for choose_arg in choose_args:
                id2choose_args[choose_arg['bucket_id']][name] = choose_arg
        del self.crushmap['choose_args']

        def walk(bucket):
            if bucket['id'] in id2choose_args:
                bucket['choose_args'] = id2choose_args[bucket['id']]
            for child in bucket.get('children', []):
                walk(child)
        for root in self.crushmap['trees']:
            walk(root)
        return True

    def _split_choose_args(self):
        assert 'choose_args' not in self.crushmap
        name2choose_args = collections.defaultdict(lambda: [])

        def walk(bucket):
            if 'choose_args' in bucket:
                for name, choose_arg in bucket['choose_args'].items():
                    assert bucket['id'] == choose_arg['bucket_id']
                    name2choose_args[name].append(choose_arg)
                del bucket['choose_args']
            for child in bucket.get('children', []):
                walk(child)
        for root in self.crushmap['trees']:
            walk(root)
        self.crushmap['choose_args'] = {}
        for name, choose_args in name2choose_args.items():
            self.crushmap['choose_args'][name] = sorted(choose_args, key=lambda v: v['bucket_id'])

    def update_choose_args(self, name, choose_args):
        if 'choose_args' not in self.crushmap:
            self.crushmap['choose_args'] = {name: choose_args}
            return
        if name not in self.crushmap['choose_args']:
            self.crushmap['choose_args'][name] = choose_args
            return
        u = {}
        for choose_arg in self.crushmap['choose_args'][name] + choose_args:
            u[choose_arg['bucket_id']] = choose_arg
        self.crushmap['choose_args'][name] = sorted(u.values(), key=lambda v: v['bucket_id'])

    def filter(self, fun, root):
        names = self.crushmap.get('choose_args', {}).keys()
        self._merge_choose_args()

        def walk(bucket):
            for pos in reversed(range(len(bucket.get('children', [])))):
                if not fun(bucket['children'][pos]):
                    child = bucket['children'][pos]
                    del bucket['children'][pos]
                    if 'choose_args' in bucket:
                        for name, choose_arg in bucket['choose_args'].items():
                            if 'ids' in choose_arg:
                                del choose_arg['ids'][pos]
                            if 'weight_set' in choose_arg:
                                for weights in choose_arg['weight_set']:
                                    del weights[pos]
            for child in bucket.get('children', []):
                walk(child)
        walk(root)

        self._split_choose_args()
        if 'choose_args' in self.crushmap:
            for name in names:
                if name not in self.crushmap['choose_args']:
                    self.crushmap['choose_args'][name] = []

    @staticmethod
    def collect_paths(children, path):
        children_info = []
        for child in children:
            child_path = copy.copy(path)
            child_path[child.get('type', 'device')] = child['name']
            children_info.append(child_path)
            if child.get('children'):
                children_info.extend(Crush.collect_paths(child['children'], child_path))
        return children_info

    def collect_item2path(self, children):
        paths = self.collect_paths(children, collections.OrderedDict())
        item2path = {}
        for path in paths:
            elements = list(path.values())
            item2path[elements[-1]] = elements
        return item2path

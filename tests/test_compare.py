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
import copy
import logging
from pprint import pprint
import pytest  # noqa import pytest

from crush.main import Main
from crush import Crush

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCompare(object):

    def test_sanity_check_args(self):
        a = Main().constructor([
            'compare',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --origin' in str(e.value)

        a = Main().constructor([
            'compare',
            '--origin', 'ORIGIN',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --destination' in str(e.value)

        a = Main().constructor([
            'compare',
            '--origin', 'ORIGIN',
            '--destination', 'DESTINATION',
        ])
        a.pre_sanity_check_args()
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'missing --rule' in str(e.value)

        a = Main().constructor([
            'compare',
            '--origin', 'ORIGIN',
            '--destination', 'DESTINATION',
            '--rule', 'RULE',
            '--choose-args', 'CHOOSE ARGS',
        ])
        a.pre_sanity_check_args()
        a.post_sanity_check_args()

    def setup_class(self):
        pass

    def define_crushmap_10(self):
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
                "indep": [
                    ["take", "dc1"],
                    ["chooseleaf", "indep", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "children": [
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 1},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 2},
                ],
            } for i in range(0, 10)
        ])
        return crushmap

    def define_crushmaps_1(self):
        crushmap = self.define_crushmap_10()
        pprint(crushmap)
        c1 = Crush()
        c1.parse(crushmap)

        m2 = copy.deepcopy(crushmap)
        del m2['trees'][0]['children'][2]['children'][1]
        c2 = Crush()
        c2.parse(m2)

        return (c1, c2)

    def test_display(self):
        c1, c2 = self.define_crushmaps_1()
        c = Main().constructor([
            'compare',
            '--rule', 'indep',
            '--replication-count', '2',
            '--values-count', '10',
        ])
        c.set_origin(c1)
        c.set_destination(c2)
        c.compare()
        out = c.display()
        print(out)
        assert 'device04        0        0        1        1   10.00%' in out
        assert 'objects%   10.00%    5.00%    5.00%    5.00%   25.00%' in out

    def test_compare_bucket_firstn(self):
        origin = self.define_crushmap_10()
        pprint(origin)

        # firstn, mapping order does not matter
        c = Main().constructor([
            '--verbose',
            'compare',
            '--rule', 'firstn',
            '--replication-count', '2',
        ])

        c.set_origin_crushmap(origin)

        #
        # Swapping weights within a bucket, values stay
        # in the bucket but move between children
        #
        destination = copy.deepcopy(origin)
        host0 = destination['trees'][0]['children'][0]
        w0 = host0['children'][0]['weight']
        w1 = host0['children'][1]['weight']
        host0['children'][0]['weight'] = w1
        host0['children'][1]['weight'] = w0
        c.set_destination_crushmap(destination)

        c.args.values_count = 100
        (from_to, in_out) = c.compare_bucket(host0)
        assert from_to == {
            'device01': {'device00': 4}
        }
        assert in_out == {}

        #
        # Increasing the weight of the items it contains changes the
        # weight of the bucket and items move in/out of the bucket.
        #
        print("collisions")
        destination = copy.deepcopy(origin)
        host0 = destination['trees'][0]['children'][0]
        host0['children'][0]['weight'] *= 10
        c.set_destination_crushmap(destination)

        c.args.values_count = 10
        (from_to, in_out) = c.compare_bucket(host0)
        print("from_to " + str(from_to))
        print("in_out " + str(in_out))
        assert from_to == {
            'device01': {'device00': 1}
        }
        assert in_out == {
            'device05': {'device00': 1},
            'device08': {'device00': 1},
            'device04': {'device00': 1},
        }

    def test_compare_bucket_indep(self):
        origin = self.define_crushmap_10()

        # indep, mapping order does not matter
        c = Main().constructor([
            'compare',
            '--rule', 'indep',
            '--replication-count', '2',
            '--order-matters',
        ])

        c.set_origin_crushmap(origin)

        #
        # Swapping weights within a bucket, values stay
        # in the bucket but move between children
        #
        destination = copy.deepcopy(origin)
        host0 = destination['trees'][0]['children'][0]
        w0 = host0['children'][0]['weight']
        w1 = host0['children'][1]['weight']
        host0['children'][0]['weight'] = w1
        host0['children'][1]['weight'] = w0
        c.set_destination_crushmap(destination)

        c.args.values_count = 100
        (from_to, in_out) = c.compare_bucket(host0)
        assert from_to == {
            'device01': {'device00': 5}
        }
        assert in_out == {}

        #
        # Increasing the weight of the items it contains changes the
        # weight of the bucket and items move in/out of the bucket.
        #
        destination = copy.deepcopy(origin)
        host0 = destination['trees'][0]['children'][0]
        host0['children'][0]['weight'] *= 10
        c.set_destination_crushmap(destination)

        c.args.values_count = 30
        (from_to, in_out) = c.compare_bucket(host0)
        print("from_to " + str(from_to))
        print("in_out " + str(in_out))
        assert from_to == {
            'device01': {'device00': 1}
        }
        assert in_out == {
            'device01': {'device05': 1},
            'device04': {'device00': 1},
            'device05': {'device00': 2},
            'device09': {'device01': 1},
            'device13': {'device00': 1},
            'device17': {'device00': 1},
            'device19': {'device00': 1},
            'device00': {'device19': 1},
        }

    def test_compare(self):
        c1, c2 = self.define_crushmaps_1()

        # firstn, mapping order does not matter
        c = Main().constructor([
            'compare',
            '--rule', 'firstn',
            '--replication-count', '2',
            '--values-count', '10',
        ])

        # device05 is removed
        c.set_origin(c1)
        c.set_destination(c2)
        assert c.compare() == {
            'device04': {'device17': 1},
            'device05': {'device12': 1, 'device04': 2, 'device08': 1}
        }
        # device05 is added
        c.set_origin(c2)
        c.set_destination(c1)
        assert c.compare() == {
            'device12': {'device05': 1},
            'device17': {'device04': 1},
            'device04': {'device05': 2},
            'device08': {'device05': 1}
        }

        # indep, mapping order matters
        c = Main().constructor([
            'compare',
            '--rule', 'indep',
            '--replication-count', '2',
            '--values-count', '10',
            '--order-matters',
        ])
        # device05 is removed
        c.set_origin(c1)
        c.set_destination(c2)
        assert c.compare() == {
            'device04': {'device13': 1, 'device17': 1},
            'device05': {'device04': 2, 'device08': 1}
        }
        # device05 is added
        c.set_origin(c2)
        c.set_destination(c1)
        assert c.compare() == {
            'device13': {'device04': 1},
            'device17': {'device04': 1},
            'device04': {'device05': 2},
            'device08': {'device05': 1}
        }

    def define_crushmaps_2(self):
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
                "indep": [
                    ["take", "dc1"],
                    ["chooseleaf", "indep", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "children": [
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 50},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 50},
                ],
            } for i in range(0, 5)
        ])
        first = crushmap['trees'][0]['children'][0]['children']
        first[0]['weight'] = 5
        first[1]['weight'] = 5
        pprint(crushmap)
        c1 = Crush()
        c1.parse(crushmap)

        m2 = copy.deepcopy(crushmap)
        del m2['trees'][0]['children'][4]
        c2 = Crush()
        c2.parse(m2)

        return (c1, c2)

    def test_compare_display(self):
        c = Main().constructor([
            'compare',
            '--rule', 'firstn',
            '--values-count', '1000',
        ])
        c1, c2 = self.define_crushmaps_2()
        c.set_origin(c2)
        c.set_destination(c1)
        c.args.replication_count = 1
        c.compare()
        out = c.display()
        print(out)
        assert "objects%   10.20%   12.70%   22.90%" in out
        c.args.replication_count = 3
        c.compare()
        out = c.display()
        print(out)
        assert ("objects%    0.17%    0.13%    0.97%    0.77%    0.77%"
                "    0.63%    0.73%    0.87%   12.30%   12.20%   29.53%") in out

    def test_origin_weights(self):
        a = Main().constructor([
            "compare", "--rule", "replicated_ruleset",
            "--replication-count", "1",
            "--origin", "tests/weights-crushmap.json",
            "--destination", "tests/weights-crushmap.json",
            "--origin-weights", "tests/weights.json"])
        a.args.backward_compatibility = True
        a.run_compare()

    def test_destination_weights(self):
        a = Main().constructor([
            "compare", "--rule", "replicated_ruleset",
            "--replication-count", "1",
            "--origin", "tests/weights-crushmap.json",
            "--destination", "tests/weights-crushmap.json",
            "--destination-weights", "tests/weights.json"])
        a.args.backward_compatibility = True
        a.run_compare()

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -vv -s tests/test_compare.py"
# End:

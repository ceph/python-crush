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

from crush.compare import Compare
from crush.main import Main
from crush import Crush

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCompare(object):

    def setup_class(self):
        pass

    def define_crushmaps_1(self):
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
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 1.0},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 2.0},
                ],
            } for i in range(0, 10)
        ])
        pprint(crushmap)
        c1 = Crush(verbose=1)
        c1.parse(crushmap)

        m2 = copy.deepcopy(crushmap)
        del m2['trees'][0]['children'][2]['children'][1]
        c2 = Crush(verbose=1)
        c2.parse(m2)

        return (c1, c2)

    def test_display(self):
        c1, c2 = self.define_crushmaps_1()
        c = Compare.factory([
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

    def test_compare(self):
        c1, c2 = self.define_crushmaps_1()

        # firstn, mapping order does not matter
        c = Compare.factory([
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
        c = Compare.factory([
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
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 5.0},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 5.0},
                ],
            } for i in range(0, 5)
        ])
        first = crushmap['trees'][0]['children'][0]['children']
        first[0]['weight'] = 0.5
        first[1]['weight'] = 0.5
        pprint(crushmap)
        c1 = Crush(verbose=1)
        c1.parse(crushmap)

        m2 = copy.deepcopy(crushmap)
        del m2['trees'][0]['children'][4]
        c2 = Crush(verbose=1)
        c2.parse(m2)

        return (c1, c2)

    def test_compare_display(self):
        c = Main().constructor([
            '--verbose',
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
        assert ("objects%    0.20%    0.17%    1.23%    1.03%"
                "    1.23%    1.10%    0.77%    1.00%   11.90%   12.50%   31.13%") in out

# Local Variables:
# compile-command: "cd .. ; virtualenv/bin/tox -e py27 -- -vv -s tests/test_compare.py"
# End:

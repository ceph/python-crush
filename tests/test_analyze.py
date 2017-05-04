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
import logging
import pytest # noqa needed for capsys

from crush import Crush
from crush.main import Main
from crush.analyze import Analyze

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestAnalyze(object):

    def test_collect_dataframe(self):
        tree = {
            'name': 'rack0', 'type': 'rack', 'id': -1, 'children': [
                {'name': 'host0', 'type': 'host', 'id': -2, 'children': [
                    {'name': 'osd.3', 'id': 3},
                ]},
                {'name': 'host1', 'type': 'host', 'id': -3, 'children': [
                    {'name': 'osd.4', 'id': 4},
                ]},
            ]
        }
        c = Crush(verbose=1)
        c.parse({"trees": [tree]})
        d = Analyze.collect_dataframe(c, tree)
        expected = """\
        ~id~  ~weight~  ~type~   rack   host device
~name~                                             
rack0     -1       1.0    rack  rack0    NaN    NaN
host0     -2       1.0    host  rack0  host0    NaN
osd.3      3       1.0  device  rack0  host0  osd.3
host1     -3       1.0    host  rack0  host1    NaN
osd.4      4       1.0  device  rack0  host1  osd.4\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

    def test_analyze(self):
        trees = [
            {"name": "dc1", "type": "root", "id": -1, 'children': []},
        ]
        weights = (
            (10.0, 1.0, 5.0, 4.0),
            (10.0, 1.0, 5.0, 4.0),
            (10.0, 1.0, 5.0, 4.0),
            (10.0, 1.0, 5.0, 4.0),
            (1.0, 0.1, 0.5, 0.4),
        )
        trees[0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 3),
                "name": "host%d" % i,
                "weight": weights[i][0],
                "children": [
                    {"id": (3 * i),
                     "name": "device%02d" % (3 * i), "weight": weights[i][1]},
                    {"id": (3 * i + 1),
                     "name": "device%02d" % (3 * i + 1), "weight": weights[i][2]},
                    {"id": (3 * i + 2),
                     "name": "device%02d" % (3 * i + 2), "weight": weights[i][3]},
                ],
            } for i in range(5)
        ])
        a = Main().constructor([
            'analyze',
            '--rule', 'data',
            '--replication-count', '2',
            '--values-count', '10000',
        ])
        a.args.crushmap = {
            "trees": trees,
            "rules": {
                "data": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", "host"],
                    ["emit"]
                ]
            }
        }
        d = a.analyze()
        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under used %~
~name~                                                
host4     -7       1.0        541                10.91
host3     -6      10.0       4930                 1.07
host2     -5      10.0       4860                -0.37
host1     -4      10.0       4836                -0.86
host0     -3      10.0       4833                -0.92

Worst case scenario if a host fails:

        ~over used %~
~type~               
device          25.55
host            22.45
root             0.00\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

    def test_analyze_weights(self):
        a = Main().constructor(
            ["analyze", "--rule", "replicated_ruleset",
             "--replication-count", "2", "--type", "device",
             "--crushmap", "tests/ceph/dump.json",
             "--weights", "tests/ceph/weights.json"])
        a.args.backward_compatibility = True
        res = a.run()
        assert "-100.00" in str(res)  # One of the OSDs has a weight of 0.0

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_analyze.py"
# End:

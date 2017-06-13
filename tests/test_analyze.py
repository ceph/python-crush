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
import pytest # noqa needed for caplog

from crush import Crush
from crush.main import Main
from crush.analyze import Analyze

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestAnalyze(object):

    def test_sanity_check_args(self):
        a = Main().constructor([
            'analyze',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --crushmap' in str(e.value)

        a = Main().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
        ])
        a.pre_sanity_check_args()

        a = Main().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
        ])
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'missing --rule' in str(e.value)

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

    def make_analyze(self, size, host_weight):
        host_weight = [w * 0x10000 for w in host_weight]
        p = [
            '--replication-count', str(size),
            '--values-count', '2048',
        ]

        hosts_count = len(host_weight)
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(host_weight),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "weight": host_weight[i],
                "children": [],
            } for i in range(0, hosts_count)
        ])
        a = Main().constructor([
            'analyze',
            '--rule', 'firstn',
        ] + p)
        a.args.crushmap = crushmap
        return a

    def test_analyze_out_of_bounds(self):

        weights = [7, 7, 7, 3, 3]
        print("==== weights " + str(weights))
        a = self.make_analyze(4, weights)
        d = a.analyze_report(*a.analyze())
        print(str(d))
        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under filled %~
~name~                                                  
host4     -6       3.0       1329                  29.79
host3     -5       3.0       1325                  29.39
host1     -3       7.0       1857                 -23.61
host0     -2       7.0       1848                 -24.05
host2     -4       7.0       1833                 -24.78

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
host                0.0
root                0.0

The following are overweighted and should be cropped:

        ~id~  ~weight~  ~cropped weight~  ~cropped %~
~name~                                               
host0     -2       7.0          393216.0        14.29
host1     -3       7.0          393216.0        14.29
host2     -4       7.0          393216.0        14.29\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

        weights = [5, 1, 1, 1, 1]
        print("==== weights " + str(weights))
        a = self.make_analyze(2, weights)
        d = a.analyze_report(*a.analyze())
        print(str(d))

        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under filled %~
~name~                                                  
host4     -6       1.0        617                  20.51
host3     -5       1.0        612                  19.53
host2     -4       1.0        593                  15.82
host1     -3       1.0        584                  14.06
host0     -2       5.0       1690                 -37.48

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
host              17.19
root               0.00

The following are overweighted and should be cropped:

        ~id~  ~weight~  ~cropped weight~  ~cropped %~
~name~                                               
host0     -2       5.0          262144.0         20.0\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

        weights = [7, 7, 3, 1, 1, 1]
        print("==== weights " + str(weights))
        a = self.make_analyze(3, weights)
        d = a.analyze_report(*a.analyze())
        print(str(d))
        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under filled %~
~name~                                                  
host3     -5       1.0        468                  37.11
host4     -6       1.0        461                  35.06
host5     -7       1.0        451                  32.13
host2     -4       3.0       1215                  18.65
host0     -2       7.0       1791                 -26.83
host1     -3       7.0       1758                 -28.45

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
host              30.86
root               0.00

The following are overweighted and should be cropped:

        ~id~  ~weight~  ~cropped weight~  ~cropped %~
~name~                                               
host0     -2       7.0          393216.0        14.29
host1     -3       7.0          393216.0        14.29\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

        weights = [5, 5, 3, 3, 3]
        print("==== weights " + str(weights))
        a = self.make_analyze(4, weights)
        d = a.analyze_report(*a.analyze())
        print(str(d))
        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under filled %~
~name~                                                  
host3     -5       3.0       1535                  12.43
host4     -6       3.0       1504                  10.16
host2     -4       3.0       1484                   8.69
host0     -2       5.0       1835                 -20.40
host1     -3       5.0       1834                 -20.45

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
host                0.0
root                0.0

The following are overweighted and should be cropped:

        ~id~  ~weight~  ~cropped weight~  ~cropped %~
~name~                                               
host0     -2       5.0          294912.0         10.0
host1     -3       5.0          294912.0         10.0\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

    def test_analyze_bad_failure_domain(self, caplog):
        a = Main().constructor(
            [
                "analyze",
                "--rule", "replicated_ruleset",
                "--replication-count", "3",
                "--crushmap", "tests/ineffective-failure-domain-crushmap.json"
            ])
        d = a.run()
        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under filled %~
~name~                                                  
kensi     -2     33.52     100000                    0.0
alicia    -3     33.52     100000                    0.0
maze      -4     33.52     100000                    0.0\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)
        assert 'not enough host' in caplog.text()

    def test_analyze(self):
        trees = [
            {"name": "dc1", "type": "root", "id": -1, 'children': []},
        ]
        weights = (
            (100 * 0x10000, 10 * 0x10000, 50 * 0x10000, 40 * 0x10000),
            (100 * 0x10000, 10 * 0x10000, 50 * 0x10000, 40 * 0x10000),
            (100 * 0x10000, 10 * 0x10000, 50 * 0x10000, 40 * 0x10000),
            (100 * 0x10000, 10 * 0x10000, 50 * 0x10000, 40 * 0x10000),
            (10 * 0x10000, 1 * 0x10000, 5 * 0x10000, 4 * 0x10000),
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
        d = a.analyze_report(*a.analyze())
        print(str(d))
        expected = """\
        ~id~  ~weight~  ~objects~  ~over/under filled %~
~name~                                                  
host4     -7      10.0        541                  10.91
host3     -6     100.0       4930                   1.07
host2     -5     100.0       4860                  -0.37
host1     -4     100.0       4836                  -0.86
host0     -3     100.0       4833                  -0.92

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
device            25.55
host              22.45
root               0.00\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

    def test_analyze_weights(self):
        a = Main().constructor(
            ["analyze", "--rule", "replicated_ruleset",
             "--replication-count", "2", "--type", "device",
             "--crushmap", "tests/weights-crushmap.json",
             "--weights", "tests/weights.json"])
        a.args.backward_compatibility = True
        res = a.run()
        assert "-100.00" in str(res)  # One of the OSDs has a weight of 0.0

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_analyze.py"
# End:

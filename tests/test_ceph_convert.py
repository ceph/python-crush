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

from crush.ceph import CephCrush, Ceph
import pytest


class TestConvert(object):

    def test_sanity_check_args(self):
        a = Ceph().constructor([
            'convert',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --in-path' in str(e.value)

        a = Ceph().constructor([
            'convert',
            '--in-path', 'IN',
        ])
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'missing --out-path' in str(e.value)

        a = Ceph().constructor([
            'convert',
            '--in-path', 'IN',
            '--out-path', 'OUT',
        ])
        a.pre_sanity_check_args()
        a.post_sanity_check_args()

    def define_crushmap(self, host_count):
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
                "weight": 3,
                "children": [
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 1},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 2},
                ],
            } for i in range(0, host_count)
        ])
        return crushmap

    def test_ceph_version_compat(self):
        crushmap = self.define_crushmap(2)
        first_weight = 123
        second_weight = 456
        crushmap['choose_args'] = {
            0: [
                {
                    'bucket_id': -1,
                    'weight_set': [
                        [first_weight, second_weight],
                    ]
                }
            ]
        }
        c = CephCrush()
        c.parse(crushmap)
        c.ceph_version_compat()
        compat = c.get_crushmap()
        assert 'choose_args' not in compat
        assert compat['trees'][1]['name'] == 'dc1-target-weight'
        bucket = compat['trees'][0]
        assert bucket['children'][0]['weight'] == first_weight
        assert bucket['children'][1]['weight'] == second_weight

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -vv -s tests/test_ceph_convert.py"
# End:

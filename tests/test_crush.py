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

logging.getLogger('crush').setLevel(logging.DEBUG)


class TestCrush(object):

    def build_crushmap(self):
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
                "data": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", "host"],
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
        return crushmap

    def test_map(self):
        crushmap = self.build_crushmap()
        c = Crush(verbose=1)
        assert c.parse(crushmap)
        assert len(c.map(rule="data", value=1234, replication_count=1,
                         weights={}, choose_args=[])) == 1

    def test_get_item_by_(self):
        crushmap = self.build_crushmap()
        c = Crush(verbose=1)
        assert c.parse(crushmap)
        assert c.get_item_by_id(-2)['name'] == 'host0'
        assert c.get_item_by_name('host0')['id'] == -2

    def test_convert_to_crushmap(self, caplog):
        crushmap = {}
        assert crushmap == Crush._convert_to_crushmap(crushmap)
        crushmap = Crush._convert_to_crushmap("tests/sample-crushmap.json")
        assert 'trees' in crushmap
        crushmap = Crush._convert_to_crushmap("tests/sample-ceph-crushmap.txt")
        assert 'trees' in crushmap
        crushmap = Crush._convert_to_crushmap("tests/sample-ceph-crushmap.crush")
        assert 'trees' in crushmap
        crushmap = Crush._convert_to_crushmap("tests/sample-ceph-crushmap.json")
        assert 'trees' in crushmap
        with pytest.raises(ValueError) as e:
            crushmap = Crush._convert_to_crushmap("tests/sample-bugous-crushmap.json")
        assert "Expecting property name" in str(e.value)

    def test_parse_weights_file(self):

        # Test Simple weights file
        weights = Crush.parse_weights_file(open("tests/ceph/weights.json"))
        assert weights == {"osd.0": 0.0, "osd.2": 0.5}

        # Test OSDMap
        weights = Crush.parse_weights_file(open("tests/ceph/osdmap.json"))
        assert weights == {"osd.0": 1.0, "osd.1": 0.95, "osd.2": 1.0}

        with pytest.raises(AssertionError):
            Crush.parse_weights_file(open("tests/ceph/weights-notfloat.json"))
        with pytest.raises(AssertionError):
            Crush.parse_weights_file(open("tests/ceph/osdmap-invalid.json"))
        with pytest.raises(AssertionError):
            Crush.parse_weights_file(open("tests/sample-ceph-crushmap.txt"))

    def test_filter_real(self):
        name = 'cloud6-1429'
        c = Crush()
        c.parse('tests/test_crush_filter.json')
        crushmap = c.get_crushmap()
        assert 3 == len(crushmap['choose_args']['optimize'])
        assert -2 == crushmap['choose_args']['optimize'][1]['bucket_id']
        assert 7 == len(crushmap['choose_args']['optimize'][0]['weight_set'][0])
        bucket = c.find_bucket(name)
        assert name == bucket['name']
        c.filter(lambda x: x.get('name') != name, crushmap['trees'][0])
        assert 2 == len(crushmap['choose_args']['optimize'])
        assert -3 == crushmap['choose_args']['optimize'][1]['bucket_id']
        assert 6 == len(crushmap['choose_args']['optimize'][0]['weight_set'][0])
        assert c.find_bucket(name) is None

    def test_filter_basic(self):
        root = {
            'name': 'root',
            'children': [
                {'name': 'bucket1', 'children': [{'id': 1}, {'id': 2}, {'id': 4}]},
                {'name': 'bucket2'},
                {'name': 'bucket3', 'id': -1, 'children': [{'id': 5}, {'id': 6}, {'id': 7}]},
            ]
        }
        expected_root = {
            'name': 'root',
            'children': [
                {'name': 'bucket1', 'children': [{'id': 1}]},
                {'name': 'bucket2'},
                {'name': 'bucket3', 'id': -1, 'children': [{'id': 5}, {'id': 7}]},
            ]
        }
        choose_args = [
            {'bucket_id': -1, 'ids': [15, 16, 17]},
            {'bucket_id': -12, 'ids': [100, 101, 102]},
            {'bucket_name': 'bucket3', 'ids': [11, 12, 14],
             'weight_set': [[11.0, 12.0, 14.0]]}]
        expected_choose_args = [
            {'bucket_id': -1, 'ids': [15, 17]},
            {'bucket_id': -12, 'ids': [100, 101, 102]},
            {'bucket_name': 'bucket3', 'ids': [11, 14],
             'weight_set': [[11.0, 14.0]]}]
        c = Crush()
        c.crushmap = {}
        c.crushmap['trees'] = [root]
        c.crushmap['choose_args'] = {"one": choose_args}

        def fun(x):
            if x.get('id') and x.get('id') % 2 == 0:
                return False
            return True

        c.filter(fun, root)
        assert expected_root == root
        assert expected_choose_args == choose_args

    def test_collect_buckets_by_type(self):
        children = [
            {
                'type': 'host',
                'name': 'host0',
            },
            {
                'type': 'rack',
                'children': [
                    {
                        'name': 'host1',
                        'type': 'host',
                    },
                    {
                        'name': 'host2',
                        'type': 'other',
                    }
                ],
            }
        ]
        expected = [{'name': 'host0', 'type': 'host'}, {'name': 'host1', 'type': 'host'}]
        assert expected == Crush.collect_buckets_by_type(children, 'host')

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_crush.py"
# End:

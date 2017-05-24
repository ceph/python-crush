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
import pytest # noqa needed for caplog

from crush import Crush


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
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 1},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 2},
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

    def test_convert_to_crushmap(self):
        c = Crush()
        crushmap = {}
        assert crushmap == c._convert_to_crushmap(crushmap)
        crushmap = c._convert_to_crushmap("tests/sample-crushmap.json")
        assert 'trees' in crushmap
        with pytest.raises(ValueError) as e:
            crushmap = c._convert_to_crushmap("tests/sample-bugous-crushmap.json")
        assert "Expecting property name" in str(e.value)

    def test_parse_weights_file(self):

        # Test Simple weights file
        weights = Crush.parse_weights_file(open("tests/weights.json"))
        assert weights == {"osd.0": 0.0, "osd.2": 0.5}

        # Test OSDMap
        weights = Crush.parse_weights_file(open("tests/ceph/osdmap.json"))
        assert weights == {"osd.1": 0.95}

        with pytest.raises(AssertionError):
            Crush.parse_weights_file(open("tests/ceph/weights-notfloat.json"))
        with pytest.raises(Exception):
            Crush.parse_weights_file(open("tests/ceph/osdmap-invalid.json"))
        with pytest.raises(AssertionError):
            Crush.parse_weights_file(open("tests/sample-ceph-crushmap.txt"))

    def test_merge_split_choose_args(self):
        c = Crush()
        split = {
            'choose_args': {
                'a': [
                    {'bucket_id': -3},
                    {'bucket_id': -2},
                    {'bucket_id': -1},
                ],
                'b': [
                    {'bucket_id': -1},
                ]
            },
            'trees': [
                {
                    'id': -1,
                    'children': [{
                        'id': -3,
                        'children': [{'id': -4}]
                    }],
                },
                {'id': -2},
            ]
        }
        merged = {
            'trees': [
                {
                    'id': -1,
                    'children': [{
                        'id': -3,
                        'children': [{'id': -4}],
                        'choose_args': {'a': {'bucket_id': -3}},
                    }],
                    'choose_args': {'a': {'bucket_id': -1}, 'b': {'bucket_id': -1}},
                },
                {'id': -2, 'choose_args': {'a': {'bucket_id': -2}}}
            ]
        }

        c.crushmap = copy.deepcopy(split)
        assert c._merge_choose_args()
        assert merged == c.crushmap
        # do nothing if no choose_args
        assert c._merge_choose_args() is False
        c._split_choose_args()
        assert split == c.crushmap

    def test_filter_real(self):
        name = 'cloud6-1429'
        c = Crush()
        c.parse('tests/test_crush_filter.json')
        crushmap = c.get_crushmap()
        optimize = sorted(crushmap['choose_args']['optimize'], key=lambda v: v['bucket_id'])
        assert 3 == len(optimize)
        assert -1 == optimize[2]['bucket_id']
        assert 7 == len(optimize[2]['weight_set'][0])
        bucket = c.find_bucket(name)
        assert name == bucket['name']
        c.filter(lambda x: x.get('name') != name, crushmap['trees'][0])
        optimize = crushmap['choose_args']['optimize']
        assert 2 == len(optimize)
        assert -1 == optimize[1]['bucket_id']
        assert 6 == len(optimize[1]['weight_set'][0])
        assert c.find_bucket(name) is None

    def test_filter_basic(self):
        root = {
            'name': 'root',
            'id': -5,
            'children': [
                {'name': 'bucket2', 'id': -3},
                {'name': 'bucket1', 'id': -2, 'children': [{'id': 1}, {'id': 2}, {'id': 4}]},
                {'name': 'bucket3', 'id': -1, 'children': [{'id': 5}, {'id': 6}, {'id': 7}]},
            ]
        }
        expected_root = {
            'name': 'root',
            'id': -5,
            'children': [
                {'name': 'bucket2', 'id': -3},
                {'name': 'bucket1', 'id': -2, 'children': [{'id': 1}]},
                {'name': 'bucket3', 'id': -1, 'children': [{'id': 5}, {'id': 7}]},
            ]
        }
        choose_args = [
            {'bucket_id': -2, 'ids': [11, 12, 14],
             'weight_set': [[11.0, 12.0, 14.0]]},
            {'bucket_id': -1, 'ids': [15, 16, 17]},
        ]
        expected_choose_args = [
            {'bucket_id': -2, 'ids': [11],
             'weight_set': [[11.0]]},
            {'bucket_id': -1, 'ids': [15, 17]},
        ]
        c = Crush()
        c.crushmap = {}
        c.crushmap['trees'] = [root]
        c.crushmap['choose_args'] = {
            "one": choose_args,
            "empty": [],
        }

        def fun(x):
            if x.get('id') in (2, 4, 6):
                return False
            return True

        c.filter(fun, root)
        assert expected_root == root
        assert expected_choose_args == choose_args
        assert "empty" in c.crushmap['choose_args']

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

    def test_update_choose_args(self):
        c = Crush()
        c.crushmap = {}
        choose_args = [{"bucket_id": -1}]
        c.update_choose_args('name', choose_args)
        assert choose_args == c.crushmap['choose_args']['name']

        c.update_choose_args('other_name', choose_args)
        assert choose_args == c.crushmap['choose_args']['other_name']

        choose_args = [{"bucket_id": -2}]
        c.update_choose_args('name', choose_args)
        expected = [{'bucket_id': -2}, {'bucket_id': -1}]
        assert expected == c.crushmap['choose_args']['name']

        choose_args[0]['modified'] = True
        c.update_choose_args('name', choose_args)
        expected = [{'bucket_id': -2, 'modified': True}, {'bucket_id': -1}]
        assert expected == c.crushmap['choose_args']['name']


# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_crush.py"
# End:

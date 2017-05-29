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
import pytest

from crush.ceph import CephCrushmapConverter, CephCrush


class TestCephCrush(object):

    def test_convert_to_crushmap(self):
        c = CephCrush()
        crushmap = {}
        assert crushmap == c._convert_to_crushmap(crushmap)
        crushmap = c._convert_to_crushmap("tests/sample-crushmap.json")
        assert 'trees' in crushmap
        crushmap = c._convert_to_crushmap("tests/sample-ceph-crushmap.txt")
        assert 'trees' in crushmap
        crushmap = c._convert_to_crushmap("tests/sample-ceph-crushmap.crush")
        assert 'trees' in crushmap
        crushmap = c._convert_to_crushmap("tests/sample-ceph-crushmap.json")
        assert 'trees' in crushmap
        with pytest.raises(ValueError) as e:
            crushmap = c._convert_to_crushmap("tests/sample-bugous-crushmap.json")
        assert "Expecting property name" in str(e.value)


class TestCephCrushmapConverter(object):

    def test_recover_choose_args(self):
        ceph = {
            'buckets': [
                {
                    'name': 'SELF-target-weight',
                    'id': -10,
                    'items': [
                        {'weight': 1 * 0x10000, 'id': 1},
                        {'weight': 2 * 0x10000, 'id': 2}
                    ]
                },
                {
                    'name': 'SELF',
                    'id': -1,
                    'items': [
                        {'weight': 10 * 0x10000, 'id': 1},
                        {'weight': 20 * 0x10000, 'id': 2}
                    ]
                },
            ]
        }
        CephCrushmapConverter.recover_choose_args(ceph)
        expected = {
            'choose_args': {' placeholder ': [{'bucket_id': -1, 'weight_set': [[10, 20]]}]},
            'buckets': [
                {
                    'name': 'SELF',
                    'id': -1,
                    'items': [
                        {'weight': 1 * 0x10000, 'id': 1},
                        {'weight': 2 * 0x10000, 'id': 2}
                    ]
                },
            ]
        }
        assert expected == ceph

    def test_recover_choose_args_added(self):
        ceph = {
            'buckets': [
                {
                    'name': 'SELF-target-weight',
                    'id': -10,
                    'items': [
                        {'weight': 1 * 0x10000, 'id': 1},
                    ]
                },
                {
                    'name': 'SELF',
                    'id': -1,
                    'items': [
                        {'weight': 10 * 0x10000, 'id': 1},
                        {'weight': 2 * 0x10000, 'id': 2}
                    ]
                },
            ]
        }
        CephCrushmapConverter.recover_choose_args(ceph)
        expected = {
            'choose_args': {' placeholder ': [{'bucket_id': -1, 'weight_set': [[10, 0]]}]},
            'buckets': [
                {
                    'name': 'SELF',
                    'id': -1,
                    'items': [
                        {'weight': 1 * 0x10000, 'id': 1},
                        {'weight': 2 * 0x10000, 'id': 2}
                    ]
                },
            ]
        }
        assert expected == ceph

    def test_recover_choose_args_removed(self):
        ceph = {
            'buckets': [
                {
                    'name': 'SELF-target-weight',
                    'id': -10,
                    'items': [
                        {'weight': 1 * 0x10000, 'id': 1},
                        {'weight': 2 * 0x10000, 'id': 2}
                    ]
                },
                {
                    'name': 'SELF',
                    'id': -1,
                    'items': [
                        {'weight': 20 * 0x10000, 'id': 2}
                    ]
                },
            ]
        }
        CephCrushmapConverter.recover_choose_args(ceph)
        expected = {
            'choose_args': {' placeholder ': [{'bucket_id': -1, 'weight_set': [[20]]}]},
            'buckets': [
                {
                    'name': 'SELF',
                    'id': -1,
                    'items': [
                        {'weight': 2 * 0x10000, 'id': 2}
                    ]
                },
            ]
        }
        assert expected == ceph

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_ceph_crush.py"
# End:

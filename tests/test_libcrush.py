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

from crush.libcrush import LibCrush

STEP_BACKWARDS = [
    "choose_local_tries",
    "choose_local_fallback_tries",
    "chooseleaf_vary_r",
    "chooseleaf_stable",
]
PARSE_BACKWARDS = STEP_BACKWARDS + [
    "chooseleaf_descend_once",
    "straw_calc_version",
]


class TestLibCrush(object):

    def test_parse_types_invalid(self):
        wrong = {
            'types': 1
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a list' in str(e.value)

        wrong = {
            'types': [
                1
            ]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a dict' in str(e.value)

        wrong = {
            'types': [
                {}
            ]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'missing type_id' in str(e.value)

        wrong = {
            'types': [
                {
                    'type_id': []
                }
            ]
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

        wrong = {
            'types': [
                {
                    'type_id': 1
                }
            ]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'missing name' in str(e.value)

        wrong = {
            'types': [
                {
                    'type_id': 1,
                    'name': []
                }
            ]
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_tunables(self, capsys):
        total_tries = 1234
        crushmap = {
            'tunables': {
                'choose_total_tries': total_tries,
            }
        }
        assert LibCrush(verbose=1).parse(crushmap)
        out, err = capsys.readouterr()
        assert 'choose_total_tries = ' + str(total_tries) in out

    def test_parse_tunables_invalid(self):
        wrong = {
            'tunables': 1
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a dict' in str(e.value)

        wrong = {
            'tunables': {
                1: 0
            }
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

        wrong = {
            'tunables': {
                'choose_total_tries': 'wrong'
            }
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_tunables_backward_compatibity(self, capsys):
        for backward in PARSE_BACKWARDS:
            crushmap = {
                'tunables': {
                    backward: 1234,
                }
            }
            with pytest.raises(RuntimeError) as e:
                LibCrush(verbose=1).parse(crushmap)
            assert 'not allowed unless backward_compatibility' in str(e.value)
            LibCrush(verbose=1, backward_compatibility=1).parse(crushmap)

    def test_parse_verbose(self, capsys):
        empty = {
            'trees': [],
        }
        assert LibCrush(verbose=1).parse(empty)
        out, err = capsys.readouterr()
        assert 'trees' in out

        assert LibCrush().parse(empty)
        out, err = capsys.readouterr()
        assert 'trees' not in out

    def test_parse_empty(self):
        LibCrush().parse({})

    def test_parse_no_argument(self):
        with pytest.raises(TypeError):
            LibCrush().parse()

    def test_parse_argument_wrong(self):
        with pytest.raises(TypeError):
            LibCrush().parse([])

    def test_parse_trees_type_wrong(self):
        wrong = {
            'trees': {}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a list' in str(e.value)

    def test_parse_invalid_algorithm(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'algorithm': 'FOOBAR',
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'not FOOBAR' in str(e.value)
        wrong = {
            'trees': [{
                'type': 'root',
                'algorithm': 0
            }]
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

    def test_parse_straw_algorithm(self):
        """Test that LibCrush only parses the straw algorithm
        if backward_compatibility is set to True"""
        straw_map = {
            'trees': [{
                'type': 'root',
                'name': 'cluster',
                'algorithm': 'straw'
            }]
        }

        # Backward compatibility off: fails
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(straw_map)
        assert 'straw requires backward_compatibility' in str(e.value)

        # Backward compatibility on: it works
        LibCrush(backward_compatibility=True).parse(straw_map)

    def test_parse_trees_invalid_key(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                1: 'some',
            }]
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_invalid_children(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'children': 1,
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a list' in str(e.value)

    def test_parse_invalid_children_element(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'children': [1],
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a dict' in str(e.value)

    def test_parse_bucket_invalid_key(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'INVALID': 1,
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'INVALID is not' in str(e.value)

    def test_parse_device_invalid_key(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'children': [{
                    'id': 1,
                    'name': 'device0',
                    'INVALID': 1,
                }]
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "'INVALID' is not" in str(e.value)

    def test_parse_invalid_name(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 1,
            }]
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_invalid_type(self):
        wrong = {
            'trees': [{
                'type': 1,
            }]
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_invalid_id(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': "some",
            }]
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_device_id_invalid(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'children': [{
                    'name': 'device0',
                    'id': -1,
                }],
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a positive integer" in str(e.value)

    def test_parse_bucket_id_invalid(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': 2,
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a negative integer" in str(e.value)

    def test_parse_bucket_duplicate_id(self):
        duplicate_id = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [
                    {
                        'type': 'host',
                        'name': 'host0',
                        'id': -2,
                    },
                    {
                        'type': 'host',
                        'name': 'host1',
                        'id': -2,
                    }
                ],
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(duplicate_id)
        assert ' -17 ' in str(e.value)

    def test_parse_bucket_reference_id(self):
        crushmap = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [
                    {
                        'weight': 1 * 0x10000,
                        'reference_id': -2,
                    },
                    {
                        'type': 'host',
                        'name': 'host1',
                        'id': -2,
                    }
                ],
            }]
        }
        LibCrush(verbose=1).parse(crushmap)

    def test_parse_bucket_reference_id_bad(self):
        crushmap = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [
                    {
                        'weight': 1 * 0x10000,
                        'reference_id': 'bad',
                    }
                ],
            }]
        }
        with pytest.raises(TypeError):
            LibCrush(verbose=1).parse(crushmap)

        crushmap = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [
                    {
                        'weight': 1 * 0x10000,
                        'reference_id': 1,
                        'INVALID': 'foo',
                    }
                ],
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(crushmap)
        assert 'INVALID is not among' in str(e.value)

    def test_parse_weight_invalid(self):
        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'weight': "some",
            }]
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be an int' in str(e.value)

    def test_parse_rules_type_wrong(self):
        wrong = {
            'rules': []
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a dict' in str(e.value)

    def test_parse_rules_invalid_key(self):
        wrong = {
            'rules': {
                1: 'root',
            }
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_step_missing_operand(self):
        wrong = {
            'rules': {
                'data': [
                    []
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'missing operand' in str(e.value)

    def test_parse_step_unknown_operand(self):
        wrong = {
            'rules': {
                'data': [
                    ['unknown']
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'operand unknown' in str(e.value)

    def test_parse_step_operand_bad_type(self):
        wrong = {
            'rules': {
                'data': [
                    [1]
                ]
            }
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_step_take_bad(self):
        wrong = {
            'rules': {
                'data': [
                    ["take"]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'exactly two' in str(e.value)

        wrong = {
            'rules': {
                'data': [
                    ["take", 1]
                ]
            }
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

        wrong = {
            'rules': {
                'data': [
                    ["take", u"unknown"]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'not a known bucket' in str(e.value)

    def test_parse_step_emit_bad(self):
        wrong = {
            'rules': {
                'data': [
                    ["emit", 2]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'exactly one' in str(e.value)

    def test_parse_step_set_backward(self):
        for backward in STEP_BACKWARDS:
            crushmap = {
                'rules': {
                    'data': [
                        ["set_" + backward, 1234]
                    ]
                }
            }
            with pytest.raises(RuntimeError) as e:
                LibCrush(verbose=1).parse(crushmap)
            assert 'not allowed unless backward_compatibility' in str(e.value)
            LibCrush(verbose=1, backward_compatibility=1).parse(crushmap)

    def test_parse_step_set_bad(self):
        wrong = {
            'rules': {
                'data': [
                    ["set_choose_tries"]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'exactly two' in str(e.value)

        wrong = {
            'rules': {
                'data': [
                    ["set_wrong", 1]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'set operand unknown' in str(e.value)

        wrong = {
            'rules': {
                'data': [
                    ["set_choose_tries", "LK"]
                ]
            }
        }
        with pytest.raises(Exception) as e:
            LibCrush().parse(wrong)

    def test_parse_step_choose(self):
        wrong = {
            'rules': {
                'data': [
                    ["choose"]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'exactly five' in str(e.value)

        wrong = {
            'rules': {
                'data': [
                    ["choose", 1, 2, 3, 4]
                ]
            }
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

        wrong = {
            'rules': {
                'data': [
                    ["choose", "notgood", 2, 3, 4]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'unknown notgood' in str(e.value)

        wrong = {
            'rules': {
                'data': [
                    ["choose", "firstn", "notgood", 3, 4]
                ]
            }
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

        wrong = {
            'rules': {
                'data': [
                    ["choose", "firstn", 0, "notgood", 4]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be 'type'" in str(e.value)

        wrong = {
            'rules': {
                'data': [
                    ["choose", "firstn", 0, "type", "unknown"]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "type is unknown" in str(e.value)

    def test_parse_choose_args(self):
        wrong = {
            'choose_args': 0
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a dict" in str(e.value)

        wrong = {
            'choose_args': {
                "1": 0
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a list" in str(e.value)

        wrong = {
            'choose_args': {
                "1": [
                    0,
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a dict" in str(e.value)

        wrong = {
            'choose_args': {
                "1": [
                    {
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "bucket_id or bucket_name are required" in str(e.value)

        wrong = {
            'choose_args': {
                "1": [
                    {
                        "bucket_id": []
                    },
                ]
            }
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

        wrong = {
            'choose_args': {
                "1": [
                    {
                        "bucket_id": 5
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "id must be a negative integer" in str(e.value)

        wrong = {
            'choose_args': {
                "1": [
                    {
                        "bucket_name": "unknown"
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "not a known bucket" in str(e.value)

        wrong = {
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -3
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "not in [0,0[" in str(e.value)

        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [],
            }],
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -1,
                        "ids": 0
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a list" in str(e.value)

        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [],
            }],
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -1,
                        "weight_set": 0
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a list" in str(e.value)

        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [],
            }],
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -1,
                        "ids": [50]
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "expected a list of ids with 0 elements and got 1 instead" in str(e.value)

        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [],
            }],
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -1,
                        "weight_set": [[10 * 0x10000]]
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "expected a list of weights with 0 elements and got 1 instead" in str(e.value)

        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [],
            }],
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -1,
                        "weight_set": [0]
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a list" in str(e.value)

        wrong = {
            'trees': [{
                'type': 'root',
                'name': 'dc1',
                'id': -1,
                'children': [
                    {
                        "id": 0,
                        "name": "device1",
                    }
                ],
            }],
            'choose_args': {
                "1": [
                    {
                        "bucket_id": -1,
                        "weight_set": [["bad"]]
                    },
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be an int" in str(e.value)

    def test_map_ok_choose_args(self):
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
                "for_validation": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", 0],
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
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 1 * 0x10000},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 2 * 0x10000},
                ],
            } for i in range(0, 10)
        ])
        crushmap['choose_args'] = {
            "1": [
                {
                    "bucket_name": "host9",
                    "weight_set": [[2 * 0x10000, 1 * 0x10000]]  # invert the weights
                },
            ]
        }
        c = LibCrush(verbose=1)
        assert c.parse(crushmap)
        assert c.map(rule="data",
                     value=1234,
                     replication_count=2) == ["device19", "device13"]
        assert c.map(rule="data",
                     value=1234,
                     replication_count=2,
                     choose_args="1") == ["device18", "device13"]
        assert c.map(rule="data",
                     value=1234,
                     replication_count=2,
                     choose_args=crushmap['choose_args']["1"]) == ["device18", "device13"]

    def test_map_ok(self):
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
                "for_validation": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", 0],
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
                    {"id": (2 * i), "name": "device%02d" % (2 * i), "weight": 1 * 0x10000},
                    {"id": (2 * i + 1), "name": "device%02d" % (2 * i + 1), "weight": 2 * 0x10000},
                ],
            } for i in range(0, 10)
        ])
        c = LibCrush(verbose=1)
        assert c.parse(crushmap)
        assert c.map(rule="data",
                     value=1234,
                     replication_count=1) == ["device19"]
        assert c.map(rule="data",
                     value=1234,
                     replication_count=2) == ["device19", "device13"]

    def test_map_bad(self):
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "children": [
                        {
                            "id": -2,
                            "name": "host0",
                            "type": "host",
                            "children": [
                                {"id": 0, "name": "device0", "weight": 1 * 0x10000},
                                {"id": 1, "name": "device1", "weight": 2 * 0x10000},
                            ]
                        }
                    ],
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
                ]
            }
        }
        c = LibCrush(verbose=1)
        assert c.parse(crushmap)
        assert c.map(rule="firstn",
                     value=1234,
                     replication_count=2) == ["device1"]
        assert c.map(rule="indep",
                     value=1234,
                     replication_count=2) == ["device1", None]

    def test_map_missing_map(self):
        with pytest.raises(RuntimeError) as e:
            LibCrush().map(rule="data", value=1234, replication_count=2)
        assert 'call parse()' in str(e.value)

    def test_map_no_rule(self):
        c = LibCrush(verbose=1)
        assert c.parse({})
        with pytest.raises(RuntimeError) as e:
            c.map(rule="norule",
                  value=1234,
                  replication_count=1)
        assert 'norule is not found' in str(e.value)

    def test_map_bad_replication_count(self):
        c = LibCrush(verbose=1)
        assert c.parse({
            "rules": {"data": []}
        })
        with pytest.raises(RuntimeError) as e:
            c.map(rule="data",
                  value=1234,
                  replication_count=0)
        assert 'must be >= 1' in str(e.value)

    def test_map_wrong_weights(self):
        c = LibCrush(verbose=1)
        assert c.parse({
            "rules": {"data": []}
        })
        with pytest.raises(RuntimeError) as e:
            c.map(rule="data",
                  value=1234,
                  replication_count=1,
                  weights={"unknowndevice": 1 * 0x10000})
        assert 'unknowndevice is not a known device' in str(e.value)

        assert c.parse({
            "trees": [{
                "type": "root",
                "name": "dc1",
                "children": [{
                    "name": "device0",
                    "id": 0
                }]
            }],
            "rules": {"data": []}
        })
        with pytest.raises(TypeError) as e:
            c.map(rule="data",
                  value=1234,
                  replication_count=1,
                  weights={"device0": "abc"})

    def test_map_fail(self):
        crushmap = {
            "trees": [{
                "type": "root",
                "name": "dc1",
                "id": -1,
                "children": [{
                    "type": "host",
                    "name": "host0",
                    "id": -2
                }]
            }],
            "rules": {
                "data": [
                    ["take", "dc1"],
                    ["chooseleaf", "firstn", 0, "type", "host"],
                    ["emit"]
                ]
            }
        }
        c = LibCrush(verbose=1)
        assert c.parse(crushmap)
        with pytest.raises(RuntimeError) as e:
            c.map(rule="data",
                  value=1234,
                  replication_count=1)
        assert 'unable to map' in str(e.value)

    def test_convert(self):
        c = LibCrush(verbose=1)
        crushmap = c.ceph_read("tests/sample-ceph-crushmap.txt")
        assert 'devices' in crushmap
        crushmap = c.ceph_read("tests/sample-ceph-crushmap.crush")
        assert 'devices' in crushmap

    def test_pool_pps(self):
        c = LibCrush()

        pps_1 = c.ceph_pool_pps(0, 16, 16)
        assert 430787817 == pps_1['0.0']
        pps_1_values = sorted(set(pps_1.values()))
        assert 16 == len(pps_1)

        pps_2 = c.ceph_pool_pps(0, 23, 16)
        pps_2_values = sorted(set(pps_2.values()))
        assert pps_1_values == pps_2_values

    def test_ceph_incompat(self):
        c = LibCrush(verbose=1)

        assert c.ceph_incompat() is False

        c.parse({})
        assert c.ceph_incompat() is False

        c.parse({'choose_args': {
            1: [],
            2: [],
        }})
        assert c.ceph_incompat() is True

        c.parse({'choose_args': {
            1: [],
        }})
        assert c.ceph_incompat() is False

        with pytest.raises(RuntimeError) as e:
            c.parse({'choose_args': {
                "1": [],
            }})
            assert c.ceph_incompat() is False
        assert 'Invalid argument' in str(e.value)

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_libcrush.py"
# End:

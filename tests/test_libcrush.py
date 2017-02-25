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
import json
import pytest

from crush.libcrush import LibCrush


class TestLibCrush(object):

    def test_parse_verbose(self, capsys):
        empty = {
            'trees': {"dc1": {
                '~type~': 'root',
            }}
        }
        assert LibCrush(verbose=1).parse(empty)
        out, err = capsys.readouterr()
        assert 'trees' in out
        assert '~type~' in out

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
            'trees': []
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be a dict' in str(e.value)

    def test_parse_invalid_algorithm(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~algorithm~': 'FOOBAR',
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'not FOOBAR' in str(e.value)
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~algorithm~': 0
            }}
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

    def test_parse_trees_invalid_key(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                1: 'some',
            }}
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_bucket_invalid_key(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~INVALID': 1,
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert '~INVALID is not' in str(e.value)

    def test_parse_device_invalid_key(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                'device0': {
                    '~id~': 1,
                    'INVALID': 1,
                }
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "'INVALID' is not" in str(e.value)

    def test_parse_invalid_type(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 1,
            }}
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_invalid_id(self):
        wrong = {
            'trees': {"dc1": {
                '~id~': "some",
            }}
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

    def test_parse_device_id_invalid(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                'device0': {
                    '~id~': -1,
                }
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a positive integer" in str(e.value)

    def test_parse_bucket_id_invalid(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~id~': 2,
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "must be a negative integer" in str(e.value)

    def test_parse_bucket_duplicate_id(self):
        duplicate_id = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~id~': -1,
                'host0': {
                    '~type~': 'host',
                    '~id~': -2,
                },
                'host1': {
                    '~type~': 'host',
                    '~id~': -2,
                },
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(duplicate_id)
        assert ' -17 ' in str(e.value)

    def test_parse_weight_invalid(self):
        wrong = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~weight~': "some",
            }}
        }
        with pytest.raises(TypeError):
            LibCrush().parse(wrong)

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
                    ["choose", "firstn", -1, 3, 4]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert 'must be positive' in str(e.value)

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
                    ["choose", "firstn", 0, "type", 4]
                ]
            }
        }
        with pytest.raises(TypeError) as e:
            LibCrush().parse(wrong)

        wrong = {
            'rules': {
                'data': [
                    ["choose", "firstn", 0, "type", "unknown"]
                ]
            }
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong)
        assert "type unknown is unknown" in str(e.value)

    def test_parse_various_ok(self):
        map = """
        {
          "trees": { "dc1": {
           "~id~": -1,
           "~type~": "root",
           "~algorithm~": "list",
           "rack0": {
            "~id~": -2,
            "~type~": "rack",
            "host0": {
             "~id~": -3,
             "~type~": "host",
             "device0": { "~id~": 0, "~weight~": 1.0 },
             "device2": { "~id~": 1, "~weight~": 2.0 }
            },
            "host1": {
             "~id~": -4,
             "~type~": "host",
             "device3": { "~id~": 2, "~weight~": 2.0 },
             "device4": { "~id~": 3, "~weight~": 2.0 }
            }
           },
           "rack1": {
            "~id~": -5,
            "~type~": "rack",
            "host2": {
             "~id~": -6,
             "~type~": "host",
             "device5": { "~id~": 4, "~weight~": 1.0 },
             "device6": { "~id~": 5, "~weight~": 1.0 }
            },
            "host3": {
             "~id~": -7,
             "~type~": "host",
             "device7": { "~id~": 6, "~weight~": 1.0 },
             "device8": { "~id~": 7, "~weight~": 1.0 }
            }
           }
          } },
          "rules": {
           "data": [
             [ "take", "dc1" ],
             [ "chooseleaf", "firstn", 0, "type", "host" ],
             [ "emit" ]
           ]
          }
        }

        """
        assert LibCrush(verbose=1).parse(json.loads(map))

    def test_map_ok(self):
        map = """
        {
          "trees": {
            "dc1": {
              "~type~": "root",
              "~id~": -1,
              "host0": {
                "~type~": "host",
                "~id~": -2,
                "device0": { "~id~": 0, "~weight~": 1.0 },
                "device1": { "~id~": 1, "~weight~": 2.0 }
              },
              "host1": {
                "~type~": "host",
                "~id~": -3,
                "device2": { "~id~": 2, "~weight~": 1.0 },
                "device3": { "~id~": 3, "~weight~": 2.0 }
              },
              "host2": {
                "~type~": "host",
                "~id~": -4,
                "device4": { "~id~": 4, "~weight~": 1.0 },
                "device5": { "~id~": 5, "~weight~": 2.0 }
              }
            }
          },
          "rules": {
            "data": [
              [ "take", "dc1" ],
              [ "chooseleaf", "firstn", 0, "type", "host" ],
              [ "emit" ]
            ]
          }
        }

        """
        c = LibCrush(verbose=1)
        assert c.parse(json.loads(map))
        assert c.map(rule="data",
                     value=1234,
                     replication_count=1) == ["device1"]
        assert c.map(rule="data",
                     value=1234,
                     replication_count=2) == ["device1", "device5"]

    def test_map_bad(self):
        map = """
        {
          "trees": {
            "dc1": {
              "~type~": "root",
              "~id~": -1,
              "host0": {
                "~type~": "host",
                "~id~": -2,
                "device0": { "~id~": 0, "~weight~": 1.0 },
                "device1": { "~id~": 1, "~weight~": 2.0 }
              }
            }
          },
          "rules": {
            "firstn": [
              [ "take", "dc1" ],
              [ "chooseleaf", "firstn", 0, "type", "host" ],
              [ "emit" ]
            ],
            "indep": [
              [ "take", "dc1" ],
              [ "chooseleaf", "indep", 0, "type", "host" ],
              [ "emit" ]
            ]
          }
        }

        """
        c = LibCrush(verbose=1)
        assert c.parse(json.loads(map))
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
                  weights={"unknowndevice": 1.0})
        assert 'unknowndevice is not a known device' in str(e.value)

        assert c.parse({
            "trees": {"dc1": {
                "~type~": "root",
                "device0": {
                    "~id~": 0
                }
            }},
            "rules": {"data": []}
        })
        with pytest.raises(TypeError) as e:
            c.map(rule="data",
                  value=1234,
                  replication_count=1,
                  weights={"device0": "abc"})

    def test_map_fail(self):
        map = """
        {
          "trees": {
            "dc1": {
              "~type~": "root",
              "~id~": -1,
              "host0": {
                "~type~": "host",
                "~id~": -2
              }
            }
          },
          "rules": {
            "data": [
              [ "take", "dc1" ],
              [ "chooseleaf", "firstn", 0, "type", "host" ],
              [ "emit" ]
            ]
          }
        }

        """
        c = LibCrush(verbose=1)
        assert c.parse(json.loads(map))
        with pytest.raises(RuntimeError) as e:
            c.map(rule="data",
                  value=1234,
                  replication_count=1)
        assert 'unable to map' in str(e.value)

# Local Variables:
# compile-command: "cd .. ; tox -e py27 tests/test_libcrush.py"
# End:

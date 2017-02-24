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

    def test_parse_invalid_algorithm(self):
        wrong_algorithm = {
            'trees': {"dc1": {
                '~type~': 'root',
                '~algorithm~': 'FOOBAR',
            }}
        }
        with pytest.raises(RuntimeError) as e:
            LibCrush().parse(wrong_algorithm)
        assert 'not FOOBAR' in str(e.value)

    def test_parse_duplicate_bucket_id(self):
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

# Local Variables:
# compile-command: "cd .. ; tox -e py27 tests/test_libcrush.py"
# End:

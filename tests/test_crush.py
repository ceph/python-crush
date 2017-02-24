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
import pytest # noqa needed for capsys

from crush import Crush


class TestCrush(object):

    def test_parse_empty(self, capsys):
        empty = {
            'trees': {"dc1": {
                '~type~': 'root',
            }}
        }
        assert Crush(verbose=True).parse(empty)
        out, err = capsys.readouterr()
        assert 'trees' in out
        assert '~type~' in out

        assert Crush().parse(empty)
        out, err = capsys.readouterr()
        assert 'trees' not in out

    def test_map(self):
        map = """
        {
          "trees": { "dc1": {
           "~type~": "root",
           "host0": {
            "~type~": "host",
            "device0": { "~id~": 0, "~weight~": 1.0 },
            "device1": { "~id~": 1, "~weight~": 2.0 }
           },
           "host1": {
            "~type~": "host",
            "device2": { "~id~": 2, "~weight~": 1.0 },
            "device3": { "~id~": 3, "~weight~": 2.0 }
           },
           "host2": {
            "~type~": "host",
            "device4": { "~id~": 4, "~weight~": 1.0 },
            "device5": { "~id~": 5, "~weight~": 2.0 }
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
        c = Crush(verbose=1)
        assert c.parse(json.loads(map))
        assert len(c.map(rule="data", value=1234, replication_count=1)) == 1

# Local Variables:
# compile-command: "cd .. ; tox -e py27 tests/test_crush.py"
# End:

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
import pytest # noqa needed for capsys

from crush import Crush


class TestCrush(object):

    def test_map(self):
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
        c = Crush(verbose=1)
        assert c.parse(crushmap)
        assert len(c.map(rule="data", value=1234, replication_count=1)) == 1

# Local Variables:
# compile-command: "cd .. ; virtualenv/bin/tox -e py27 tests/test_crush.py"
# End:

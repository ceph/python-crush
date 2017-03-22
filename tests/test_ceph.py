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
import logging
import os
import pytest # noqa needed for capsys

from crush.main import Main

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCeph(object):

    def test_convert(self, capsys):
        Main().run([
            'ceph',
            '--convert', 'tests/ceph.json',
        ])
        out, err = capsys.readouterr()
        assert '"reference_id": -2' in out

    def test_convert_text(self, capsys):
        Main().run(['ceph',
                    '--convert-text', 'tests/map.txt',
                    '--output', 'tests/map.json'])
        out, err = capsys.readouterr()
        assert not out
        assert not err

        with open('tests/map.json') as f_json:
            json_map = json.load(f_json)
            assert 'devices' in json_map
            assert 'types' in json_map
            assert 'buckets' in json_map
        os.unlink('tests/map.json')

    def test_invalid_args(self):
        with pytest.raises(SystemExit):
            Main().run(['ceph', '--convert-text', 'tests/map.txt'])

        with pytest.raises(SystemExit):
            Main().run(['ceph', '--output', 'tests/map.json'])

        with pytest.raises(SystemExit):
            Main().run(['ceph',
                        '--convert-text', 'tests/map.txt',
                        '--output', 'tests/map.json',
                        '--convert', 'tests/ceph.json'])

# Local Variables:
# compile-command: "cd .. ; virtualenv/bin/tox -e py27 -- -vv -s tests/test_ceph.py"
# End:

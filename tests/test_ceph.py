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
import pytest  # noqa needed for capsys

from crush.ceph import Ceph

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCeph(object):

    def convert(self, ext):
        Ceph().main([
            'convert', 'tests/sample-ceph-crushmap.' + ext,
        ])

    def test_convert_json(self, capsys):
        self.convert('json')
        out, err = capsys.readouterr()
        assert '"reference_id": -2' in out

    def test_convert_txt(self, capsys):
        self.convert('txt')
        out, err = capsys.readouterr()
        assert '"reference_id": -2' in out

    def test_convert_crush(self, capsys):
        self.convert('crush')
        out, err = capsys.readouterr()
        assert '"weight": 5.0' in out

    def test_hook_create_values(self):
        c = Ceph()
        c.parse([
            '--verbose',
            'analyze',
            '--values-count', '2',
        ])
        assert {0: 0, 1: 1} == c.hook_create_values()
# Local Variables:
# compile-command: "cd .. ; virtualenv/bin/tox -e py27 -- -vv -s tests/test_ceph.py"
# End:

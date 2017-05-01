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
import os
import pytest  # noqa needed for capsys

from crush.ceph import Ceph

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCeph(object):

    def test_conversions(self):
        base = 'tests/sample-ceph-crushmap.'
        for ext_in in ('txt', 'crush', 'json', 'python-json'):
            in_path = base + ext_in
            for ext_out in ('txt', 'crush', 'json', 'python-json'):
                expected_path = base + ext_out
                out_path = expected_path + ".err"
                Ceph().main([
                    'convert',
                    '--in-path', in_path,
                    '--out-path', out_path,
                    '--out-format', ext_out,
                ])
                if ext_out == 'crush':
                    cmd = "cmp"
                else:
                    cmd = "diff -Bbu"
                assert os.system(cmd + " " + expected_path + " " + out_path) == 0
                os.unlink(out_path)

    def test_hook_create_values(self):
        c = Ceph()
        c.parse([
            '--verbose',
            'analyze',
            '--values-count', '2',
        ])
        assert {0: 0, 1: 1} == c.hook_create_values()
        c.parse([
            '--verbose',
            'analyze',
            '--pool', '2',
            '--pg-num', '3',
            '--pgp-num', '3',
        ])
        expected = {u'2.0': -113899774, u'2.1': -1215435108, u'2.2': -832918304}
        assert expected == c.hook_create_values()

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -vv -s tests/test_ceph.py"
# End:

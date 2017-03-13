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
import pytest # noqa needed for capsys

from crush.main import Main

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCeph(object):

    def test_main(self, capsys):
        Main().run([
            'ceph',
            '--convert', 'tests/ceph.json',
        ])
        out, err = capsys.readouterr()
        assert '"reference_id": -2' in out

# Local Variables:
# compile-command: "cd .. ; virtualenv/bin/tox -e py27 -- -vv -s tests/test_ceph.py"
# End:

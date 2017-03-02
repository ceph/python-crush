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

from crush import main


class TestCrush(object):

    def test_run(self):
        return  # needs a bit more love to actually work
        c = main.Crush()
        argv = []
        c.run(argv)
        assert (logging.getLogger('crush').getEffectiveLevel() ==
                logging.INFO)
        c.run(['--verbose'] + argv)
        assert (logging.getLogger('crush').getEffectiveLevel() ==
                logging.DEBUG)

# Local Variables:
# compile-command: "cd .. ; virtualenv/bin/tox -e py27 tests/test_main.py"
# End:

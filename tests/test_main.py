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
import argparse
import pickle
import pprint

from crush import analyze
from crush import main
from crush import optimize


class TestCrush(object):

    def test_run(self):
        # needs a bit more love to actually work
        pass
        # c = main.Crush()
        # argv = []
        # c.run(argv)
        # assert (logging.getLogger('crush').getEffectiveLevel() ==
        #         logging.INFO)
        # c.run(['--verbose'] + argv)
        # assert (logging.getLogger('crush').getEffectiveLevel() ==
        #         logging.DEBUG)

    def test_pickle(self):
        m = main.Main()
        m.parse(['optimize'])
        p = pickle.dumps(m)
        n = pickle.loads(p)
        assert n.argv == m.argv

    def test_get_trimmed_argv(self):
        d_parser = optimize.Optimize.get_parser()
        parser = argparse.ArgumentParser(
            parents=[
                d_parser,
            ],
            conflict_handler='resolve',
        )
        parser.add_argument('--something-with-arg')
        parser.add_argument('--no-arg', action='store_true')
        pprint.pprint(vars(parser))
        argv_discarded = [
            '--no-arg',
            '--something-with-arg', 'arg',
            '--step', '200',
        ]
        argv_preserved = [
            '--type', 'device',
        ]
        argv = argv_discarded + argv_preserved
        args = parser.parse_args(argv)
        trimmed_argv = main.Main.get_trimmed_argv(
            analyze.Analyze.get_parser(), args)
        expected = ['--type', 'device']
        assert expected == trimmed_argv


# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_main.py"
# End:

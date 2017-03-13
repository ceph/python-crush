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
import logging
import textwrap

from crush import analyze
from crush import compare
from crush import ceph

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')


class Main(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            A library to control placement in a hierarchy
            """))

        self.parser.add_argument(
            '-v', '--verbose',
            action='store_true', default=None,
            help='be more verbose',
        )

        subparsers = self.parser.add_subparsers(
            title='subcommands',
            description='valid subcommands',
            help='sub-command -h',
        )

        analyze.Analyze.set_parser(subparsers)
        compare.Compare.set_parser(subparsers)
        ceph.Ceph.set_parser(subparsers)

    def constructor(self, argv):
        self.args = self.parser.parse_args(argv)

        if self.args.verbose:
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.getLogger('crush').setLevel(level)

        return self.args.func(self.args)

    def run(self, argv):
        return self.constructor(argv).run()

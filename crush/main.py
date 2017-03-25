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

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')


class Main(object):

    def __init__(self):
        self.create_parser()

        if self.parser.get_default('backward_compatibility') is None:
            self.parser.set_defaults(backward_compatibility=False)

        self.parser.add_argument(
            '-v', '--verbose',
            action='store_true', default=None,
            help='be more verbose',
        )

        self.subparsers = self.parser.add_subparsers(
            title='subcommands',
            description='valid subcommands',
            help='sub-command -h',
        )

        analyze.Analyze.set_parser(self.subparsers, self.hook_analyze_args)
        compare.Compare.set_parser(self.subparsers, self.hook_compare_args)

    def create_parser(self):
        self.parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            A library to control placement in a hierarchy
            """))

    def parse(self, argv):
        self.args = self.parser.parse_args(argv)

        if self.args.verbose:
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.getLogger('crush').setLevel(level)

    def constructor(self, argv):
        self.parse(argv)
        return self.args.func(self.args, self)

    def main(self, argv):
        return self.constructor(argv).run()

    def hook_analyze_args(self, parser):
        pass

    def hook_compare_args(self, parser):
        pass

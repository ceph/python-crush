# -*- mode: python; coding: utf-8 -*-
#
# Copyright (C) 2017 <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see `<http://www.gnu.org/licenses/>`.
#
import argparse
import logging
import textwrap

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')


class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    pass


class Crush(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser(
            formatter_class=CustomFormatter,
            description=textwrap.dedent("""\
            A library to control placement in a hierarchy
            """))

        self.parser.add_argument(
            '-v', '--verbose',
            action='store_true', default=None,
            help='be more verbose',
        )

    def run(self, argv):
        self.args = self.parser.parse_args(argv)

        if self.args.verbose:
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.getLogger('crush').setLevel(level)

        return self.args.func(self.args).run()

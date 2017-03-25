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

from crush import main
from crush.ceph import convert

log = logging.getLogger(__name__)


class Ceph(main.Main):

    def __init__(self):
        super(Ceph, self).__init__()

        self.parser.add_argument(
            '--no-backward-compatibility',
            dest='backward_compatibility',
            action='store_false', default=True,
            help='do not allow backward compatibility tunables (default: allowed)')

        convert.Convert.set_parser(self.subparsers)

    def create_parser(self):
        self.parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Ceph crush compare and analyze
            """),
            epilog=textwrap.dedent("""
            """),
        )

    def hook_analyze_args(self, parser):
        pass

    def hook_compare_args(self, parser):
        pass

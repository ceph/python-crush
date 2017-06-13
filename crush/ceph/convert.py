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
from __future__ import division

import argparse
import logging
import textwrap


log = logging.getLogger(__name__)


class Convert(object):

    def __init__(self, args, main):
        self.args = args
        self.main = main

    @staticmethod
    def get_parser():
        parser = argparse.ArgumentParser(
            add_help=False,
            conflict_handler='resolve',
        )
        parser.add_argument(
            '--out-path',
            help='path of the output file')
        parser.add_argument(
            '--pool',
            help='pool',
            type=int)
        return parser

    @staticmethod
    def set_parser(subparsers, arguments):
        parser = Convert.get_parser()
        arguments(parser)
        subparsers.add_parser(
            'convert',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            The Ceph crushmap can be stored in three formats:

            - JSON, the output of *ceph osd crush dump* or *ceph report*

            - txt, the output of *crushtool -d*

            - binary, the output of *crushtool -c* or *ceph getcrushmap*

            The JSON used is different from the python-crush format documented at
            http://crush.readthedocs.io/en/latest/api.html#crush.Crush.parse.

            It supports any of the existing Ceph formats and is
            compatible with Luminous and below. It converts the
            crushmap into the python-crush format and display the
            result on the standard output.

            """),
            epilog=textwrap.dedent("""
            Examples:

            Convert a Ceph JSON crushmap into a python-crush crushmap:
            - crush convert --in-path crushmap-ceph.json --out-path crushmap.json

            Convert a Ceph text crushmap into a python-crush crushmap:
            - crush convert --in-path crushmap.txt --out-path crushmap.json

            Convert a binary crushmap to python-crush crushmap:
            - crush convert --in-path crushmap.bin --out-path crushmap.json

            Convert a python-crush crushmap to Ceph text crushmap
            - crush convert --in-path crushmap.json \\
                            --out-path crushmap.json --out-format txt
            """),
            help='Convert crushmaps',
            parents=[parser],
        ).set_defaults(
            func=Convert,
        )

    def pre_sanity_check_args(self):
        self.main.hook_convert_pre_sanity_check_args(self.args)

    def post_sanity_check_args(self):
        self.main.hook_convert_post_sanity_check_args(self.args)
        if not self.args.out_path:
            raise Exception("missing --out-path")

    def run(self):
        self.pre_sanity_check_args()
        crushmap = self.main.convert_to_crushmap(self.args.in_path)
        self.post_sanity_check_args()
        self.main.crushmap_to_file(crushmap)

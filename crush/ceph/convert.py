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
import json
import logging
import textwrap

from crush import Crush

log = logging.getLogger(__name__)


class Convert(object):

    def __init__(self, args, hooks):
        self.args = args
        self.hooks = hooks

    @staticmethod
    def get_parser():
        parser = argparse.ArgumentParser(
            add_help=False,
            conflict_handler='resolve',
        )
        formats = ('txt', 'json', 'python-json', 'crush')
        parser.add_argument(
            '--in-path',
            required=True,
            help='path of the input file')
        parser.add_argument(
            '--in-format',
            choices=formats,
            help='format of the input file')
        parser.add_argument(
            '--out-path',
            required=True,
            help='path of the output file')
        parser.add_argument(
            '--out-format',
            choices=formats,
            default='python-json',
            help='format of the output file')
        return parser

    @staticmethod
    def set_parser(subparsers):
        parser = Convert.get_parser()
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

    @staticmethod
    def choose_args_int_index(crushmap):
        if 'choose_args' not in crushmap:
            return crushmap
        crushmap['choose_args'] = {
            int(k): v for (k, v) in crushmap['choose_args'].items()
        }
        return crushmap

    def run(self):
        c = Crush(verbose=self.args.verbose, backward_compatibility=True)
        crushmap = c._convert_to_crushmap(self.args.in_path)
        c.parse_crushmap(crushmap)
        if self.args.out_format == 'python-json':
            open(self.args.out_path, "w").write(json.dumps(crushmap, indent=4, sort_keys=True))
        else:
            c.parse(Convert.choose_args_int_index(crushmap))
            c.c.ceph_write(self.args.out_path,
                           self.args.out_format,
                           crushmap.get('private'))

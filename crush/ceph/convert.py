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
import copy
import json
import logging
import textwrap

from crush import Crush

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
        formats = ('txt', 'json', 'python-json', 'crush')
        parser.add_argument(
            '--in-path',
            help='path of the input file')
        parser.add_argument(
            '--in-format',
            choices=formats,
            help='format of the input file')
        parser.add_argument(
            '--out-path',
            help='path of the output file')
        parser.add_argument(
            '--out-format',
            choices=formats,
            default='python-json',
            help='format of the output file')
        versions = ('hammer', 'jewel', 'kraken', 'luminous')
        parser.add_argument(
            '--out-version',
            choices=versions,
            default='luminous',
            help='version of the output file (default luminous)')
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

    def ceph_version_compat(self, c):
        #
        # sanity checks
        #
        crushmap = c.get_crushmap()
        if len(crushmap['choose_args']) > 1:
            raise Exception("expected exactly one choose_args, got " +
                            str(crushmap['choose_args'].keys()) + " instead")
        # ... if c.c.crushwapper cannot encode raise

        c._merge_choose_args()
        crushmap = c.get_crushmap()

        #
        # create the shadow trees with the target weights
        #
        self.max_bucket_id = min(c._id2item.keys())

        def rename(bucket):
            if 'children' not in bucket:
                return
            self.max_bucket_id -= 1
            bucket['id'] = self.max_bucket_id
            bucket['name'] += '-target-weight'
            if 'choose_args' in bucket:
                del bucket['choose_args']
            for child in bucket.get('children', []):
                rename(child)
        shadow_trees = copy.deepcopy(crushmap['trees'])
        for tree in shadow_trees:
            rename(tree)

        #
        # override the target weights with the weight set
        #
        def reweight(bucket):
            if 'children' not in bucket:
                return
            children = bucket['children']
            if 'choose_args' in bucket:
                choose_arg = next(iter(bucket['choose_args'].values()))
                weight_set = choose_arg['weight_set'][0]
                for i in range(len(children)):
                    children[i]['weight'] = weight_set[i]
                del bucket['choose_args']
            for child in children:
                reweight(child)
        for tree in crushmap['trees']:
            reweight(tree)

        crushmap['trees'].extend(shadow_trees)
        return crushmap

    def ceph_convert(self, c):
        if self.args.out_version == 'luminous':
            return self.choose_args_int_index(c.get_crushmap())
        if 'choose_args' not in c.get_crushmap():
            return c.get_crushmap()
        return self.ceph_version_compat(c)

    def run(self):
        c = Crush(verbose=self.args.verbose, backward_compatibility=True)
        c.parse(self.args.in_path)
        crushmap = c.get_crushmap()
        if self.args.out_format == 'python-json':
            open(self.args.out_path, "w").write(json.dumps(crushmap, indent=4, sort_keys=True))
        else:
            c.parse(self.ceph_convert(c))
            c.c.ceph_write(self.args.out_path,
                           self.args.out_format,
                           crushmap.get('private'))

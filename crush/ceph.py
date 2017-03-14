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
import json
import logging
import textwrap

from crush import Crush

log = logging.getLogger(__name__)


class Ceph(object):

    def __init__(self, args):
        self.args = args

    @staticmethod
    def get_parser():
        parser = argparse.ArgumentParser(
            add_help=False,
            conflict_handler='resolve',
        )
        parser.add_argument(
            '--convert',
            help=('convert PATH (which is expected to be a file '
                  'in Ceph JSON format as produced by CrushWrapper::dump) '
                  'into the python-crush JSON format and display '
                  'the result on the standard output'),
            metavar='PATH',
        )
        return parser

    @staticmethod
    def set_parser(subparsers):
        subparsers.add_parser(
            'ceph',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Ceph support

            The Ceph crushmap can be displayed in JSON using the following commands:

            - ceph osd crush dump
            - crushtool --dump
            - ceph report

            The JSON used is different from the python-crush format documented at
            http://crush.readthedocs.io/en/latest/api.html#crush.Crush.parse
            and can be converted using the --convert option.

            """),
            epilog=textwrap.dedent("""
            Examples:

            Convert a Ceph JSON crushmap into a python-crush crushmap:

            - ceph osd crush dump > crushmap-ceph.json
            - crush ceph --convert crushmap-ceph.json > crushmap.json

            - ceph osd getcrushmap > crushmap
            - crushtool -d crushmap -o /dev/null --dump > crushmap-ceph.json
            - crush ceph --convert crushmap-ceph.json > crushmap.json
            """),
            help='Ceph support',
            parents=[Ceph.get_parser()],
        ).set_defaults(
            func=Ceph,
        )

    @staticmethod
    def factory(argv):
        return Ceph(Ceph.get_parser().parse_args(argv))

    def run(self):
        if self.args.convert:
            self.convert(self.args.convert)

    def convert(self, input):
        converted = self.parse_ceph(json.load(open(self.args.convert)))
        c = Crush(verbose=self.args.verbose, backward_compatibility=True)
        c.parse(converted)
        print(json.dumps(converted, indent=4, sort_keys=True))

    @staticmethod
    def weight_as_float(i):
        return float(i) / float(0x10000)

    def convert_item(self, item, ceph):
        if item['id'] >= 0:
            return {
                "weight": self.weight_as_float(item['weight']),
                "id": item['id'],
                "name": ceph['id2name'][item['id']],
            }
        else:
            return {
                "weight": self.weight_as_float(item['weight']),
                "reference_id": item['id'],
            }

    def convert_bucket(self, bucket, ceph):
        b = {
            "weight": self.weight_as_float(bucket['weight']),
            "id": bucket['id'],
            "name": bucket['name'],
            "algorithm": bucket['alg'],
            "type": bucket['type_name'],
        }
        items = bucket.get('items', [])
        if items:
            children = []
            last_pos = -1
            for item in items:
                # the actual pos value does not change the mapping
                # when there is an empty item (we do not store pos)
                # but the order of the items is important and we
                # need to assert that the list is ordered
                assert last_pos < item['pos']
                last_pos = item['pos']
                children.append(self.convert_item(item, ceph))
            b['children'] = children
        return b

    def convert_rule(self, ceph_rule, ceph):
        name = ceph_rule['rule_name']
        rule = []
        for ceph_step in ceph_rule['steps']:
            if 'opcode' in ceph_step:
                if ceph_step['opcode'] in (10, 11, 12, 13):
                    id2name = {
                        10: 'set_choose_local_tries',
                        11: 'set_choose_local_fallback_tries',
                        12: 'set_chooseleaf_vary_r',
                        13: 'set_chooseleaf_stable',
                    }
                    step = [id2name[ceph_step['opcode']], ceph_step['arg1']]
                else:
                    assert 0, "unexpected rule opcode " + str(ceph_step['opcode'])
            elif 'op' in ceph_step:
                if ceph_step['op'] == 'take':
                    step = [ceph_step['op'], ceph_step['item_name']]
                elif ceph_step['op'] in ('chooseleaf_firstn',
                                         'choose_firstn',
                                         'chooseleaf_indep',
                                         'choose_indep'):
                    (choose, how) = ceph_step['op'].split('_')
                    if ceph['type2id'][ceph_step['type']] == 0:
                        type = 0
                    else:
                        type = ceph_step['type']
                    step = [choose, how, ceph_step['num'], 'type', type]
                elif ceph_step['op'] in ('set_choose_local_tries',
                                         'set_choose_local_fallback_tries',
                                         'set_chooseleaf_vary_r',
                                         'set_chooseleaf_stable',
                                         'set_choose_tries',
                                         'set_chooseleaf_tries'):
                    step = [ceph_step['op'], ceph_step['num']]
                elif ceph_step['op'] == 'emit':
                    step = ['emit']
                elif ceph_step['op'] == 'noop':
                    pass
                else:
                    assert 0, "unexpected rule op " + str(ceph_step['op'])
            else:
                assert 0, "no op or opcode found"
            rule.append(step)
        return (name, rule)

    def convert_tunables(self, tunables):
        known = set([
            'choose_local_tries',
            'choose_local_fallback_tries',
            'chooseleaf_vary_r',
            'chooseleaf_stable',
            'chooseleaf_descend_once',
            'straw_calc_version',

            'choose_total_tries',
        ])
        out = {}
        for (k, v) in tunables.items():
            if k in known:
                out[k] = v
        return out

    def parse_ceph(self, ceph):
        ceph['id2name'] = {d['id']: d['name'] for d in ceph['devices']}
        ceph['type2id'] = {t['name']: t['type_id'] for t in ceph['types']}

        j = {}

        j['trees'] = [self.convert_bucket(b, ceph) for b in ceph['buckets']]

        j['rules'] = {}
        for ceph_rule in ceph['rules']:
            (name, rule) = self.convert_rule(ceph_rule, ceph)
            j['rules'][name] = rule

        j['tunables'] = self.convert_tunables(ceph['tunables'])

        return j

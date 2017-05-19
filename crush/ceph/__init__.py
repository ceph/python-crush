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
import re
import struct
import textwrap

from crush import main
from crush.ceph import convert
from crush import Crush, LibCrush

log = logging.getLogger(__name__)


class CephConverter(object):

    @staticmethod
    def weight_as_float(i):
        return i / 0x10000

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

    def collect_items(self, children, ceph):
        for child in children:
            if 'id' in child:
                ceph['id2item'][child['id']] = child
            self.collect_items(child.get('children', []), ceph)

    def dereference(self, children, ceph):
        for i in range(len(children)):
            child = children[i]
            if 'reference_id' in child:
                id = child['reference_id']
                new_child = copy.copy(ceph['id2item'][id])
                new_child['weight'] = child['weight']
                ceph['is_child'].add(id)
                children[i] = new_child
            self.dereference(child.get('children', []), ceph)

    @staticmethod
    def recover_choose_args(ceph):
        buckets = []
        name2target_weights = {}
        has_target_weight = False
        for bucket in ceph['buckets']:
            if bucket['name'].endswith('-target-weight'):
                has_target_weight = True
                name = bucket['name'][:-14]
                name2target_weights[name] = [c['weight'] for c in bucket['children']]
            else:
                buckets.append(bucket)
        if not has_target_weight:
            return
        choose_args = []
        for bucket in buckets:
            if bucket['name'] in name2target_weights:
                target_weights = name2target_weights[bucket['name']]
                assert len(bucket['children']) == len(target_weights)
                weight_set = []
                for child in bucket['children']:
                    weight_set.append(child['weight'])
                    child['weight'] = target_weights.pop(0)
                choose_args.append({
                    'bucket_id': bucket['id'],
                    'weight_set': [weight_set],
                })
        ceph['buckets'] = buckets
        ceph['choose_args'] = {"compat": choose_args}

    def convert_buckets(self, ceph):
        ceph['is_child'] = set()
        ceph['id2item'] = {}
        self.recover_choose_args(ceph)
        converted = [self.convert_bucket(r, ceph) for r in ceph['buckets']]
        self.collect_items(converted, ceph)
        self.dereference(converted, ceph)
        return list(filter(lambda c: c['id'] not in ceph['is_child'], converted))

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

        j = {
            'private': {},
        }

        j['types'] = ceph['types']

        j['trees'] = self.convert_buckets(ceph)

        j['rules'] = {}
        for ceph_rule in ceph['rules']:
            (name, rule) = self.convert_rule(ceph_rule, ceph)
            j['rules'][name] = rule
        j['private']['rules'] = ceph['rules']

        j['tunables'] = self.convert_tunables(ceph['tunables'])

        j['private']['tunables'] = ceph['tunables']

        if 'choose_args' in ceph:
            j['choose_args'] = ceph['choose_args']

        return j


class CephCrush(Crush):

    #
    # reading a crushmap from a file
    #

    @staticmethod
    def _is_ceph_file(something):
        fmt = "I"
        crush_magic = 0x00010000
        head = open(something, mode='rb').read(struct.calcsize(fmt))
        if struct.unpack(fmt, head)[0] == crush_magic:
            return True
        content = open(something).read()
        if (re.search("^device ", content, re.MULTILINE) and
                re.search("^type ", content, re.MULTILINE)):
            return True
        return False

    @staticmethod
    def _convert_from_file(something):
        if CephCrush._is_ceph_file(something):
            crushmap = LibCrush().ceph_read(something)
            return (json.loads(crushmap), 'ceph-json')
        else:
            with open(something) as f_json:
                crushmap = json.load(f_json)
                log.debug("_detect_file_format: valid json file")
                if 'devices' in crushmap:  # Ceph json format
                    return (crushmap, 'ceph-json')
                return (crushmap, 'python-crush-json')

    @staticmethod
    def _convert_to_dict(something):
        if type(something) is dict:
            if 'devices' in something:  # Ceph json format
                return (something, 'ceph-json')
            return (something, 'python-crush-json')
        else:
            return CephCrush._convert_from_file(something)

    def _convert_to_crushmap(self, something):
        (crushmap, format) = CephCrush._convert_to_dict(something)
        if format == 'ceph-json':
            crushmap = CephConverter().parse_ceph(crushmap)
        return crushmap

    #
    # writing a crushmap to a file
    #

    @staticmethod
    def choose_args_int_index(crushmap):
        if 'choose_args' not in crushmap:
            return crushmap
        crushmap['choose_args'] = {
            int(k): v for (k, v) in crushmap['choose_args'].items()
        }
        return crushmap

    def ceph_version_compat(self):
        #
        # sanity checks
        #
        if len(self.crushmap['choose_args']) > 1:
            raise Exception("expected exactly one choose_args, got " +
                            str(self.crushmap['choose_args'].keys()) + " instead")
        # ... if c.c.crushwapper cannot encode raise

        self._merge_choose_args()
        #
        # create the shadow trees with the target weights
        #
        self.max_bucket_id = min(self._id2item.keys())

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
        shadow_trees = copy.deepcopy(self.crushmap['trees'])
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
        for tree in self.crushmap['trees']:
            reweight(tree)

        self.crushmap['trees'].extend(shadow_trees)

    def transform_to_write(self, version):
        version = ord(version[0]) - ord('a') + 1
        if version >= 12:  # 12 == luminous
            self.choose_args_int_index(self.crushmap)
            return True
        if 'choose_args' not in self.crushmap:
            return False
        self.ceph_version_compat()
        return True

    def to_file(self, path, format, version):
        if format == 'python-json':
            super(CephCrush, self).to_file(path)
        else:
            if self.transform_to_write(version):
                self.parse(self.crushmap)
            self.c.ceph_write(path, format, self.crushmap.get('private'))


class Ceph(main.Main):

    def __init__(self):
        super(Ceph, self).__init__()

        self.parser.add_argument(
            '--no-backward-compatibility',
            dest='backward_compatibility',
            action='store_false', default=True,
            help='do not allow backward compatibility tunables (default: allowed)')

        convert.Convert.set_parser(self.subparsers, self.hook_convert_args)

    def create_parser(self):
        self.parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Ceph crush compare and analyze
            """),
            epilog=textwrap.dedent("""
            """),
        )

    def clone(self):
        return Ceph()

    def hook_common_args(self, parser):
        parser.add_argument(
            '--pool',
            help='pool',
            type=int)

        parser.add_argument(
            '--pg-num',
            help='pg-num',
            type=int)

        parser.add_argument(
            '--pgp-num',
            help='pgp-num',
            type=int)

    formats = ('txt', 'json', 'python-json', 'crush')

    def in_args(self, parser):
        parser.add_argument(
            '--in-path',
            help='path of the input file')
        parser.add_argument(
            '--in-format',
            choices=Ceph.formats,
            help='format of the input file')

    def out_args(self, parser):
        parser.add_argument(
            '--out-format',
            choices=Ceph.formats,
            default='python-json',
            help='format of the output file')
        versions = ('hammer', 'jewel', 'kraken', 'luminous')
        parser.add_argument(
            '--out-version',
            choices=versions,
            default='luminous',
            help='version of the output file (default luminous)')

    def hook_convert_args(self, parser):
        self.in_args(parser)
        self.out_args(parser)

    def hook_analyze_args(self, parser):
        self.hook_common_args(parser)

    def hook_compare_args(self, parser):
        self.hook_common_args(parser)

    def hook_optimize_args(self, parser):
        self.hook_common_args(parser)
        self.out_args(parser)

    def hook_create_values(self):
        if self.args.pool is not None:
            return LibCrush().ceph_pool_pps(self.args.pool, self.args.pg_num, self.args.pgp_num)
        else:
            return super(Ceph, self).hook_create_values()

    def convert_to_crushmap(self, crushmap):
        c = CephCrush(verbose=self.args.debug,
                      backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        return c.get_crushmap()

    def crushmap_to_file(self, crushmap):
        c = CephCrush(verbose=self.args.debug,
                      backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        c.to_file(self.args.out_path, self.args.out_format, self.args.out_version)

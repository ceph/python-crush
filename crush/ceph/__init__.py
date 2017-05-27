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
import collections
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


class MappingError(Exception):
    pass


class UnsupportedError(Exception):
    pass


class HealthError(Exception):
    pass


class CephReport(object):

    def parse_report(self, report):
        if report['health']['overall_status'] != 'HEALTH_OK':
            raise HealthError("expected health overall_status == HEALTH_OK but got " +
                              report['health']['overall_status'] + "instead")
        crushmap = CephCrushmapConverter().parse_ceph(report['crushmap'])
        mappings = collections.defaultdict(lambda: {})
        for pg_stat in report['pgmap']['pg_stats']:
            mappings[pg_stat['pgid']] = pg_stat['acting']

        ruleset2name = {}
        for rule in crushmap['private']['rules']:
            ruleset2name[rule['ruleset']] = rule['rule_name']

        c = LibCrush(backward_compatibility=True)
        c.parse(crushmap)

        name2id = {}

        def collect_items(children):
            for child in children:
                if 'id' in child:
                    name2id[child['name']] = child['id']
                collect_items(child.get('children', []))
        collect_items(crushmap['trees'])

        weights = Crush.parse_osdmap_weights(report['osdmap'])

        for osd in report['osdmap']["osds"]:
            if osd["primary_affinity"] != 1.0:
                raise UnsupportedError(
                    "osd." + str(osd["osd"]) + " primary affinity is != 1.0")

        failed_mapping = False
        for pool in report['osdmap']['pools']:
            if pool['type'] != 1:
                raise UnsupportedError(
                    "pool " + pool['pool_name'] + " is type " + str(pool['type']) +
                    " is not supported, only type == 1 (replicated)")
            if pool['object_hash'] != 2:
                raise UnsupportedError(
                    "pool " + pool['pool_name'] + " object_hash " + str(pool['object_hash']) +
                    " is not supported, only object_hash == 2 (rjenkins)")
            if pool['flags_names'] != 'hashpspool':
                raise UnsupportedError(
                    "pool " + pool['pool_name'] + " has flags_names " +
                    "'" + str(pool['flags_names']) + "'" +
                    " is no supported, only hashpspool")
            ruleset = pool['crush_ruleset']
            if str(ruleset) in crushmap.get('choose_args', {}):
                choose_args = str(ruleset)
            else:
                choose_args = None
            rule = ruleset2name[ruleset]
            size = pool['size']
            log.info("verifying pool {} pg_num {} pgp_num {}".format(
                pool['pool'], pool['pg_num'], pool['pg_placement_num']))
            values = LibCrush().ceph_pool_pps(pool['pool'],
                                              pool['pg_num'],
                                              pool['pg_placement_num'])
            kwargs = {
                "rule": str(rule),
                "replication_count": size,
            }
            if choose_args:
                kwargs["choose_args"] = choose_args
            if weights:
                kwargs["weights"] = weights
            for (name, pps) in values.items():
                if name not in mappings:
                    failed_mapping = True
                    log.error(name + " is not in pgmap")
                    continue
                kwargs["value"] = pps
                mapped = c.map(**kwargs)
                osds = [name2id[x] for x in mapped]
                if osds != mappings[name]:
                    failed_mapping = True
                    log.error("{} map to {} instead of {}".format(
                        name, osds, mappings[name]))
                    continue
        if failed_mapping:
            raise MappingError("some mapping failed, please file a bug at "
                               "http://libcrush.org/main/python-crush/issues/new")
        crushmap['private']['pools'] = report['osdmap']['pools']

        (version, rest) = report['version'].split('.', 1)
        crushmap['private']['version'] = int(version)

        return crushmap


class CephCrushmapConverter(object):

    def convert_item(self, item, ceph):
        if item['id'] >= 0:
            return {
                "weight": item['weight'],
                "id": item['id'],
                "name": ceph['id2name'][item['id']],
            }
        else:
            return {
                "weight": item['weight'],
                "reference_id": item['id'],
            }

    def convert_bucket(self, bucket, ceph):
        b = {
            "weight": bucket['weight'],
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
            log.debug(str(bucket))
            if bucket['name'].endswith('-target-weight'):
                has_target_weight = True
                name = bucket['name'][:-14]
                name2target_weights[name] = [c['weight'] for c in bucket['items']]
            else:
                buckets.append(bucket)
        if not has_target_weight:
            return
        choose_args = []
        for bucket in buckets:
            if bucket['name'] in name2target_weights:
                target_weights = name2target_weights[bucket['name']]
                assert len(bucket['items']) == len(target_weights)
                weight_set = []
                for child in bucket['items']:
                    weight_set.append(child['weight'] / 0x10000)
                    child['weight'] = target_weights.pop(0)
                choose_args.append({
                    'bucket_id': bucket['id'],
                    'weight_set': [weight_set],
                })
        ceph['buckets'] = buckets
        ceph['choose_args'] = {"0": choose_args}

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

    def convert_choose_args(self, ceph):
        choose_args_map = copy.deepcopy(ceph['choose_args'])
        for (name, choose_args) in choose_args_map.items():
            for choose_arg in choose_args:
                if 'weight_set' in choose_arg:
                    choose_arg['weight_set'] = [
                        [int(x * 0x10000) for x in weights]
                        for weights in choose_arg['weight_set']
                    ]
        return choose_args_map

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
            j['choose_args'] = self.convert_choose_args(ceph)

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
                elif 'cluster_fingerprint' in crushmap:
                    return (crushmap, 'ceph-report')
                return (crushmap, 'python-crush-json')

    @staticmethod
    def _convert_to_dict(something):
        if type(something) in (dict, collections.OrderedDict):
            if 'devices' in something:
                return (something, 'ceph-json')
            elif 'cluster_fingerprint' in something:
                return (something, 'ceph-report')
            return (something, 'python-crush-json')
        else:
            return CephCrush._convert_from_file(something)

    def _convert_to_crushmap(self, something):
        (something, format) = CephCrush._convert_to_dict(something)
        if format == 'ceph-json':
            crushmap = CephCrushmapConverter().parse_ceph(something)
        elif format == 'ceph-report':
            crushmap = CephReport().parse_report(something)
        else:
            crushmap = something
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
        if self.c.ceph_incompat():
            raise Exception("choose_args cannot be encoded for a version lower than luminous")

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
        if 'choose_args' not in self.crushmap:
            return False
        self.choose_args_int_index(self.crushmap)
        self.parse(self.crushmap)
        if version >= 'luminous':
            return True
        self.ceph_version_compat()
        self.parse(self.crushmap)
        return True

    def to_file(self, path, format, version):
        if format == 'python-json':
            super(CephCrush, self).to_file(path)
        else:
            self.transform_to_write(version)
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
            default='crush',
            help='format of the output file')
        versions = ('h', 'hammer',
                    'j', 'jewel',
                    'k', 'kraken',
                    'l', 'luminous',
                    'm',
                    'n')
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

    def value_name(self):
        return 'PGs'

    def set_analyze_args(self, crushmap):
        if not hasattr(self.args, 'pool'):
            return
        if 'private' not in crushmap:
            return
        if 'pools' not in crushmap['private']:
            return

        for pool in crushmap['private']['pools']:
            if pool['pool'] == self.args.pool:
                self.args.replication_count = pool['size']
                self.argv.append('--replication-count=' + str(pool['size']))
                self.args.pg_num = pool['pg_num']
                self.argv.append('--pg-num=' + str(pool['pg_num']))
                self.args.pgp_num = pool['pg_placement_num']
                self.argv.append('--pgp-num=' + str(pool['pg_placement_num']))
                for rule in crushmap['private']['rules']:
                    if rule['ruleset'] == pool['crush_ruleset']:
                        self.args.rule = str(rule['rule_name'])
                        self.argv.append('--rule=' + str(rule['rule_name']))
        if crushmap.get('choose_args', {}).get(str(self.args.pool)):
            self.args.choose_args = str(self.args.pool)
            self.argv.append('--choose-args=' + self.args.choose_args)
        log.info('argv = ' + " ".join(self.argv))

    def set_optimize_args(self, crushmap):
        if not hasattr(self.args, 'out_version'):
            return
        if 'version' not in crushmap['private']:
            return
        self.args.out_version = chr(crushmap['private']['version'] - 1 + ord('a'))
        self.argv.append('--out-version=' + self.args.out_version)

        if self.args.out_version < 'luminous':
            self.args.with_positions = False
            self.argv.append('--no-positions')

        if not hasattr(self.args, 'pool'):
            return
        if 'private' not in crushmap:
            return
        if 'pools' not in crushmap['private']:
            return

        self.args.choose_args = str(self.args.pool)
        self.argv.append('--choose-args=' + self.args.choose_args)

        log.warning('argv = ' + " ".join(self.argv))

    def convert_to_crushmap(self, crushmap):
        c = CephCrush(verbose=self.args.debug,
                      backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        crushmap = c.get_crushmap()
        self.set_analyze_args(crushmap)
        self.set_optimize_args(crushmap)
        return crushmap

    def crushmap_to_file(self, crushmap):
        c = CephCrush(verbose=self.args.debug,
                      backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        c.to_file(self.args.out_path, self.args.out_format, self.args.out_version)

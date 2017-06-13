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
from crush import analyze
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

        v = report['version'].split('.')
        if v[0] == "0":
            if v[1] == '94':
                version = 'h'
            elif v[1] == '87':
                version = 'g'
            elif v[1] == '80':
                version = 'f'
        else:
            version = chr(ord('a') + int(v[0]) - 1)

        crushmap = CephCrushmapConverter().parse_ceph(report['crushmap'],
                                                      version=version,
                                                      recover_choose_args=False)
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

        crushmap = CephCrushmapConverter().parse_ceph(report['crushmap'],
                                                      version=version,
                                                      recover_choose_args=True)
        crushmap['private']['pools'] = report['osdmap']['pools']
        crushmap['private']['version'] = version

        return crushmap


class CephTunablesConverter(object):

    known = set([
        'choose_local_tries',
        'choose_local_fallback_tries',
        'chooseleaf_vary_r',
        'chooseleaf_descend_once',
        'straw_calc_version',

        'choose_total_tries',
    ])

    @staticmethod
    def read_tunables(tunables, version):
        known = copy.copy(CephTunablesConverter.known)
        out = {}
        if version >= 'j':
            known.add('chooseleaf_stable')
        else:
            out['chooseleaf_stable'] = 0
        for (k, v) in tunables.items():
            if k in known:
                out[k] = v
        return out

    @staticmethod
    def rewrite_tunables_txt(tunables, path, version):
        known = copy.copy(CephTunablesConverter.known)
        if version >= 'j':
            known.add('chooseleaf_stable')
        lines = list(filter(lambda l: not re.match('^tunable ', l), open(path).readlines()))
        for k in sorted(tunables.keys()):
            if k in known:
                lines.insert(0, 'tunable ' + k + ' ' + str(tunables[k]) + '\n')
        open(path, 'w').write("".join(lines))


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
        name2id = {}
        id2bucket = {}
        for bucket in ceph['buckets']:
            name2id[bucket['name']] = bucket['id']
            id2bucket[bucket['id']] = bucket
        buckets = []
        name2target_weights = {}
        has_target_weight = False
        for bucket in ceph['buckets']:
            log.debug(str(bucket))
            if bucket['name'].endswith('-target-weight'):
                has_target_weight = True
                name = bucket['name'][:-14]
                target_weights = {}
                for i in bucket['items']:
                    if i['id'] < 0:
                        c = id2bucket[i['id']]
                        assert c['name'].endswith('-target-weight')
                        child_name = c['name'][:-14]
                        id = name2id[child_name]
                    else:
                        id = i['id']
                    target_weights[id] = i['weight']
                name2target_weights[name] = target_weights
            else:
                buckets.append(bucket)
        if not has_target_weight:
            return
        choose_args = []
        for bucket in buckets:
            if bucket['name'] in name2target_weights:
                target_weights = name2target_weights[bucket['name']]
                weight_set = []
                for child in bucket['items']:
                    if child['id'] in target_weights:
                        weight_set.append(child['weight'] / 0x10000)
                        child['weight'] = target_weights[child['id']]
                    else:
                        weight_set.append(0)
                choose_args.append({
                    'bucket_id': bucket['id'],
                    'weight_set': [weight_set],
                })
        ceph['buckets'] = buckets
        ceph['choose_args'] = {" placeholder ": choose_args}

    def convert_buckets(self, ceph, recover_choose_args):
        ceph['is_child'] = set()
        ceph['id2item'] = {}
        if recover_choose_args:
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

    def parse_ceph(self, ceph, version, recover_choose_args):
        ceph['id2name'] = {d['id']: d['name'] for d in ceph['devices']}
        ceph['type2id'] = {t['name']: t['type_id'] for t in ceph['types']}

        j = {
            'private': {},
        }

        j['types'] = ceph['types']

        j['trees'] = self.convert_buckets(ceph, recover_choose_args)

        j['rules'] = {}
        for ceph_rule in ceph['rules']:
            (name, rule) = self.convert_rule(ceph_rule, ceph)
            j['rules'][name] = rule
        j['private']['rules'] = ceph['rules']

        j['tunables'] = CephTunablesConverter.read_tunables(ceph['tunables'], version)

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
            version = something.get('private', {}).get('version', 'l')
            crushmap = CephCrushmapConverter().parse_ceph(something,
                                                          version=version,
                                                          recover_choose_args=True)
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
            info = self.crushmap.get('private')
            self.c.ceph_write(path, format, info)
            if info and info.get('tunables') and format == 'txt':
                CephTunablesConverter.rewrite_tunables_txt(info['tunables'], path, version)


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

    def hook_common_post_sanity_check_args(self, args):
        if self.args.pool and self.args.values_count != analyze.Analyze.DEFAULT_VALUES_COUNT:
            raise Exception("--pool and --values-count are mutually exclusive")
        if self.args.pool:
            if not self.args.pg_num:
                raise Exception("--pg-num is required with --pool")
            if not self.args.pgp_num:
                raise Exception("--pgp-num is required with --pool")

    formats = ('txt', 'json', 'python-json', 'crush')

    def in_args(self, parser):
        parser.add_argument(
            '--in-path',
            help='path of the input file')
        parser.add_argument(
            '--in-format',
            choices=Ceph.formats,
            help='format of the input file')

    def hook_in_args_pre_sanity_check(self, args):
        if not args.in_path:
            raise Exception("missing --in-path")

    def out_args(self, parser):
        parser.add_argument(
            '--out-format',
            choices=Ceph.formats,
            default='crush',
            help='format of the output file')
        versions = ('f', 'firefly',
                    'g', 'giant',
                    'h', 'hammer',
                    'i', 'infernalis',
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

    def hook_convert_pre_sanity_check_args(self, args):
        self.hook_in_args_pre_sanity_check(args)

    def hook_convert_post_sanity_check_args(self, args):
        pass

    def hook_analyze_args(self, parser):
        self.hook_common_args(parser)

    def hook_analyze_pre_sanity_check_args(self, args):
        super(Ceph, self).hook_analyze_pre_sanity_check_args(args)

    def hook_analyze_post_sanity_check_args(self, args):
        super(Ceph, self).hook_analyze_post_sanity_check_args(args)
        self.hook_common_post_sanity_check_args(args)

    def hook_compare_args(self, parser):
        self.hook_common_args(parser)

    def hook_compare_pre_sanity_check_args(self, args):
        super(Ceph, self).hook_compare_pre_sanity_check_args(args)

    def hook_compare_post_sanity_check_args(self, args):
        super(Ceph, self).hook_compare_post_sanity_check_args(args)
        self.hook_common_post_sanity_check_args(args)

    def hook_optimize_args(self, parser):
        self.hook_common_args(parser)
        self.out_args(parser)

    def hook_optimize_pre_sanity_check_args(self, args):
        super(Ceph, self).hook_optimize_pre_sanity_check_args(args)

    def hook_optimize_post_sanity_check_args(self, args):
        super(Ceph, self).hook_optimize_post_sanity_check_args(args)
        self.hook_common_post_sanity_check_args(args)

    def hook_create_values(self):
        if self.args.pool is not None:
            return LibCrush().ceph_pool_pps(self.args.pool, self.args.pg_num, self.args.pgp_num)
        else:
            return super(Ceph, self).hook_create_values()

    def value_name(self):
        return 'PGs'

    def get_ceph_version(self, crushmap):
        if 'version' in crushmap['private']:
            return crushmap['private']['version']
        return 'l'

    def has_compat_crushmap(self, crushmap):
        return crushmap.get('choose_args', {}).get(" placeholder ") is not None

    def get_compat_choose_args(self, crushmap):
        #
        # if converting from a pre-Luminous encoded crushmap
        #
        if not self.has_compat_crushmap(crushmap):
            return None
        elif crushmap.get('private', {}).get('pools', []):
            if not hasattr(self.args, 'pool') or self.args.pool is None:
                if len(crushmap['private']['pools']) != 1:
                    raise Exception('--pool is required')
                pool = crushmap['private']['pools'][0]
                return str(pool['pool'])
            else:
                pools = []
                for pool in crushmap['private']['pools']:
                    if self.args.pool == pool['pool']:
                        return pool['pool']
                    pools.append(pool['pool'])
                raise Exception(str(self.args.pool) + " is not a known pool " + str(pools))
        else:
            return "0"

    def set_analyze_args(self, crushmap):
        if 'private' not in crushmap:
            return self.args.choose_args
        if 'pools' not in crushmap['private']:
            return self.args.choose_args

        compat_pool = self.get_compat_choose_args(crushmap)
        if (compat_pool is not None and
                (self.args.pool is None or self.args.pool == int(compat_pool))):
            self.args.pool = int(compat_pool)
            self.argv.append('--pool=' + str(self.args.pool))
            self.args.choose_args = str(self.args.pool)
            self.argv.append('--choose-args=' + self.args.choose_args)

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

        return self.args.choose_args

    def set_optimize_args(self, crushmap):
        if 'version' not in crushmap['private']:
            return self.args.choose_args
        self.args.out_version = self.get_ceph_version(crushmap)
        self.argv.append('--out-version=' + self.args.out_version)

        if self.args.out_version < 'luminous':
            self.args.with_positions = False
            self.argv.append('--no-positions')

        if not hasattr(self.args, 'pool'):
            return self.args.choose_args
        if 'private' not in crushmap:
            return self.args.choose_args
        if 'pools' not in crushmap['private']:
            return self.args.choose_args

        if self.args.choose_args is None:
            self.args.choose_args = str(self.args.pool)
            self.argv.append('--choose-args=' + self.args.choose_args)

        log.warning('argv = ' + " ".join(self.argv))

        return self.args.choose_args

    def set_compat_choose_args(self, c, crushmap, choose_args_name):

        if not self.has_compat_crushmap(crushmap):
            return

        assert choose_args_name

        choose_args = crushmap['choose_args']
        choose_args[choose_args_name] = choose_args[' placeholder ']
        del choose_args[' placeholder ']
        c.parse(crushmap)

    def convert_to_crushmap(self, crushmap):
        c = CephCrush(verbose=self.args.debug,
                      backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        crushmap = c.get_crushmap()
        if self.args.func.__name__ == 'Analyze':
            choose_args_name = self.set_analyze_args(crushmap)
        elif self.args.func.__name__ == 'Optimize':
            self.set_analyze_args(crushmap)
            choose_args_name = self.set_optimize_args(crushmap)
        elif self.args.func.__name__ == 'Convert':
            choose_args_name = self.get_compat_choose_args(crushmap)
        elif self.args.func.__name__ == 'Compare':
            choose_args_name = self.set_analyze_args(crushmap)
        else:
            raise Exception('Unexpected func=' + str(self.args.func.__name__))
        self.set_compat_choose_args(c, crushmap, choose_args_name)

        return crushmap

    def crushmap_to_file(self, crushmap):
        c = CephCrush(verbose=self.args.debug,
                      backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        c.to_file(self.args.out_path, self.args.out_format, self.args.out_version)

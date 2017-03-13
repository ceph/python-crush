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
import copy
import collections
import json
import logging
import textwrap
import pandas as pd
import numpy as np

from crush import Crush

log = logging.getLogger(__name__)


class Analyze(object):

    def __init__(self, args):
        self.args = args

    @staticmethod
    def get_parser():
        parser = argparse.ArgumentParser(
            add_help=False,
            conflict_handler='resolve',
        )
        replication_count = 3
        parser.add_argument(
            '--replication-count',
            help=('number of devices to map (default %d)' % replication_count),
            type=int,
            default=replication_count)
        parser.add_argument(
            '--rule',
            help='the name of rule')
        parser.add_argument(
            '--failure-domain',
            help='override the failure domain of the rule')
        parser.add_argument(
            '--crushmap',
            help='path to the crushmap json file')
        values_count = 100000
        parser.add_argument(
            '--values-count',
            help='repeat mapping (default %d)' % values_count,
            type=int,
            default=values_count)
        parser.add_argument(
            '--backward-compatibility',
            action='store_true', default=False,
            help='true if backward compatibility tunables are allowed (default false)')
        parser.add_argument(
            '--order-matters',
            action='store_true', default=False,
            help='true if the order of mapped devices matter (default false)')
        return parser

    @staticmethod
    def set_parser(subparsers):
        subparsers.add_parser(
            'analyze',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Analyze crushmaps

            """),
            epilog=textwrap.dedent("""
            Examples:

            """),
            help='Analyze crushmaps',
            add_help=False,
            parents=[Analyze.get_parser()],
        ).set_defaults(
            func=Analyze,
        )

    @staticmethod
    def factory(argv):
        return Analyze(Analyze.get_parser().parse_args(argv))

    @staticmethod
    def collect_paths(children, path):
        children_info = []
        for child in children:
            child_path = copy.copy(path)
            child_path[child.get('type', 'device')] = child['name']
            children_info.append(child_path)
            if child.get('children'):
                children_info.extend(Analyze.collect_paths(child['children'], child_path))
        return children_info

    @staticmethod
    def collect_item2path(children):
        paths = Analyze.collect_paths(children, collections.OrderedDict())
        item2path = {}
        for path in paths:
            elements = list(path.values())
            item2path[elements[-1]] = elements
        return item2path

    @staticmethod
    def collect_dataframe(crush, child):
        paths = Analyze.collect_paths([child], collections.OrderedDict())
        #
        # verify all paths have bucket types in the same order in the hierarchy
        # i.e. always rack->host->device and not host->rack->device sometimes
        #
        key2pos = {}
        pos2key = {}
        for path in paths:
            keys = list(path.keys())
            for i in range(len(keys)):
                key = keys[i]
                if key in key2pos:
                    assert key2pos[key] == i
                else:
                    key2pos[key] = i
                    pos2key[i] = key
        columns = []
        for pos in sorted(pos2key.keys()):
            columns.append(pos2key[pos])
        rows = []
        for path in paths:
            row = []
            for column in columns:
                element = path.get(column, np.nan)
                row.append(element)
                if element is not np.nan:
                    item_name = element
            item = crush.get_item_by_name(item_name)
            rows.append([item['id'],
                         item_name,
                         item.get('weight', 1.0),
                         item.get('type', 'device')] + row)
        d = pd.DataFrame(rows, columns=['~id~', '~name~', '~weight~', '~type~'] + columns)
        return d.set_index('~name~')

    @staticmethod
    def collect_nweight(d):
        d['~nweight~'] = 0.0
        for type in d['~type~'].unique():
            tw = float(d.loc[d['~type~'] == type, ['~weight~']].sum())
            d.loc[d['~type~'] == type, ['~nweight~']] = d['~weight~'].apply(lambda w: w / tw)
        return d

    @staticmethod
    def collect_occupation(d, total_objects):
        capacity = d['~nweight~'] * float(total_objects)
        d['~occupation~'] = 0.0
        for type in d['~type~'].unique():
            occupation = d['~objects~'] / capacity - 1.0
            d.loc[d['~type~'] == type, ['~occupation~']] = occupation * 100
        return d

    @staticmethod
    def find_take(children, item):
        for child in children:
            if child.get('name') == item:
                return child
            found = Analyze.find_take(child.get('children', []), item)
            if found:
                return found
        return None

    @staticmethod
    def analyze_rule(rule):
        take = None
        failure_domain = None
        for step in rule:
            if step[0] == 'take':
                assert take is None
                take = step[1]
            elif step[0].startswith('choose'):
                assert failure_domain is None
                (op, firstn_or_indep, num, _, failure_domain) = step
        return (take, failure_domain)

    def analyze(self):
        c = Crush(verbose=self.args.verbose,
                  backward_compatibility=self.args.backward_compatibility)
        c.parse(self.crushmap)

        crushmap = c.get_crushmap()
        trees = crushmap.get('trees', [])
        (take, failure_domain) = self.analyze_rule(crushmap['rules'][self.args.rule])
        if self.args.failure_domain:
            failure_domain = self.args.failure_domain
        root = self.find_take(trees, take)
        log.debug("root = " + str(root))
        d = self.collect_dataframe(c, root)
        d = self.collect_nweight(d)

        replication_count = self.args.replication_count
        rule = self.args.rule
        device2count = collections.defaultdict(lambda: 0)
        for value in range(0, self.args.values_count):
            m = c.map(rule, value, replication_count)
            for device in m:
                device2count[device] += 1

        item2path = self.collect_item2path([root])
        log.debug("item2path = " + str(item2path))
        d['~objects~'] = 0
        for (device, count) in device2count.items():
            for item in item2path[device]:
                d.at[item, '~objects~'] += count

        total_objects = replication_count * self.args.values_count
        d = self.collect_occupation(d, total_objects)

        s = (d['~type~'] == failure_domain) & (d['~weight~'] > 0)
        a = d.loc[s, ['~id~', '~weight~', '~occupation~']]
        return a.sort_values(by='~occupation~', ascending=False)

    def run(self):
        if self.args.crushmap:
            self.crushmap = json.load(open(self.args.crushmap))
            return self.analyze()

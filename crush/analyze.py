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
            help=('number of devices to map (default: %d)' % replication_count),
            type=int,
            default=replication_count)
        parser.add_argument(
            '--rule',
            help='the name of rule')
        parser.add_argument(
            '--type',
            help='override the type of bucket shown in the report')
        parser.add_argument(
            '--crushmap',
            help='path to the crushmap JSON file')
        values_count = 100000
        parser.add_argument(
            '--values-count',
            help='repeat mapping (default: %d)' % values_count,
            type=int,
            default=values_count)
        parser.add_argument(
            '--no-backward-compatibility',
            dest='backward_compatibility',
            action='store_false', default=True,
            help='do not allow backward compatibility tunables (default: allowed)')
        return parser

    @staticmethod
    def set_parser(subparsers):
        subparsers.add_parser(
            'analyze',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Analyze a crushmap

            Map a number of objects (--values-count) to devices (three
            by default or --replication-count if specified) using a
            crush rule (--rule) from a given crushmap (--crushmap) and
            display a report comparing the expected and the actual
            object distribution.

            The --type argument changes the item type displayed in the
            report. For instance --type device shows the individual
            OSDs and --type host shows the machines that contain
            them. If --type is not specified, it defaults to the
            "type" argument of the first "choose*" step of the rule
            selected by --rule.

            The first item in the report will be the first to become
            full. For instance if the report starts with:

                    ~id~  ~weight~   ~over/under used %~
            ~name~
            g9       -22  2.29           10.40

            it means that the bucket g9 with id -22 and weight 2.29
            will be the first bucket of its type to become full. The
            actual usage of the host will be 10.4% over the expected
            usage, i.e. if the g9 host is expected to be 70%
            full, it will actually be 80.40% full.

            The ~over/under used %~ is the variation between the
            expected item usage and the actual item usage. If it is
            positive the item is overused, if it is negative the item
            is underused. For more information about why this happens
            see http://tracker.ceph.com/issues/15653#detailed-explanation

            """),
            epilog=textwrap.dedent("""
            Examples:

            Display the first host that will become full.

            $ crush analyze --rule replicated --crushmap crushmap.json
                    ~id~  ~weight~  ~over/under used %~
            ~name~
            g9       -22  2.29     10.40
            g3        -4  1.50     10.12
            g12      -28  4.00      4.57
            g10      -24  4.98      1.95
            g2        -3  5.19      1.90
            n7        -9  5.48      1.25
            g1        -2  5.88      0.50
            g11      -25  6.22     -0.95
            g8       -20  6.67     -1.73
            g5       -15  8.79     -7.88

            Display the first device that will become full.

            $ crush analyze --type device --rule replicated \\
                            --crushmap crushmap.json
                    ~id~  ~weight~  ~over/under used %~
            ~name~
            osd.35    35  2.29     10.40
            osd.2      2  1.50     10.12
            osd.47    47  2.50      5.54
            osd.46    46  1.50      2.95
            osd.29    29  1.78      2.50
            osd.1      1  3.89      2.31
            osd.37    37  2.68      2.02
            osd.38    38  2.29      1.86
            osd.27    27  1.69      1.27
            osd.21    21  1.29      0.66
            osd.0      0  3.19      0.51
            osd.20    20  2.68      0.48
            osd.8      8  2.00      0.13
            osd.44    44  1.81     -0.15
            osd.11    11  2.59     -1.23
            osd.3      3  1.81     -1.35
            osd.9      9  4.00     -1.61
            osd.13    13  2.67     -1.90
            osd.26    26  3.00     -7.57
            osd.25    25  2.79     -7.73
            osd.24    24  3.00     -8.33
            """),
            help='Analyze crushmaps',
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
    def collect_usage(d, total_objects):
        capacity = d['~nweight~'] * float(total_objects)
        d['~over/under used %~'] = 0.0
        for type in d['~type~'].unique():
            usage = d['~objects~'] / capacity - 1.0
            d.loc[d['~type~'] == type, ['~over/under used %~']] = usage * 100
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
        if self.args.type:
            type = self.args.type
        else:
            type = failure_domain
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
        d = self.collect_usage(d, total_objects)

        s = (d['~type~'] == type) & (d['~weight~'] > 0)
        a = d.loc[s, ['~id~', '~weight~', '~over/under used %~']]
        pd.set_option('precision', 2)
        return a.sort_values(by='~over/under used %~', ascending=False)

    def run(self):
        if self.args.crushmap:
            self.crushmap = json.load(open(self.args.crushmap))
            return self.analyze()

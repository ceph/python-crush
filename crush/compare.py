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
import collections
import pandas as pd
import json
import logging
import textwrap

from crush import Crush

log = logging.getLogger(__name__)


class Compare(object):

    def __init__(self, args):
        self.args = args

    def set_origin(self, c):
        self.origin = c

    def set_destination(self, c):
        self.destination = c

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
        parser.add_argument(
            '--origin',
            metavar='PATH',
            help='PATH to the origin crushmap JSON file')
        parser.add_argument(
            '--destination',
            metavar='PATH',
            help='PATH to the destination crushmap JSON file')
        parser.add_argument(
            '--order-matters',
            action='store_true', default=False,
            help='true if the order of mapped devices matter (default: false)')
        return parser

    @staticmethod
    def set_parser(subparsers):
        subparsers.add_parser(
            'compare',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Compare crushmaps

            After a crushmap is changed (e.g. addition/removal of
            items, modification of weights or tunables), objects may
            move from an item to another.

            The crushmap before the modification is specified with the
            --origin option and the crushmap after the modification is
            specified with the --destination option.

            Each crushmap is given the same set of objects (in the
            range [0,--value-count[) to map using a given rule
            (--rule) with a given replication count
            (--replication-count). If an object is mapped to the same
            items by both crushmaps, it does not move. If it is mapped
            to different items, it moves.

            The order in which items are mapped may or may not matter.
            For instance, it does not matter if three exact copies of
            an object are mapped to items 23, 78 and 45 instead of to
            45, 78 and 23. But it matters in the case of erasure-coded
            objects because they are split among multiple items. A
            simple example: first half of object is stored in one
            item, second half is stored in a second item, and parity
            is stored in a third. A rule with a "choose*" "indep" step
            is typically used when the order of the mapping order matters
            and a rule with a "choose*" "firstn" step is most often
            used when the mapping does not matter.

            If an object maps to items 23, 78 and 45 in the --origin
            crushmap and to 45, 78 and 23 in the --destination crushmap
            (i.e. the same items but in a different order), it is assumed
            the order does not matter and the object did not move. If
            the --order-matters flag is set, it is assumed that some object
            movement will be necessary and the object will be counted as
            moving from 45 to 23 and from 23 to 45.

            """),
            epilog=textwrap.dedent("""
            Examples:

            $ crush compare --rule firstn \\
                            --replication-count 1 \\
                            --origin before.json --destination after.json
            There are 1000 objects.

            Replacing the crushmap specified with --origin with the crushmap
            specified with --destination will move 229 objects (22.9% of the total)
            from one item to another.

            The rows below show the number of objects moved from the
            given item to each item named in the columns. The objects%
            at the end of the rows shows the percentage of the total
            number of objects that is moved away from this particular
            item. The last row shows the percentage of the total
            number of objects that is moved to the item named in the
            column.

                     osd.8    osd.9    objects%
            osd.0        3        4       0.70%
            osd.1        1        3       0.40%
            osd.2       16       16       3.20%
            osd.3       19       21       4.00%
            osd.4       17       18       3.50%
            osd.5       18       23       4.10%
            osd.6       14       23       3.70%
            osd.7       14       19       3.30%
            objects%   10.20%   12.70%   22.90%

            """),
            help='Compare crushmaps',
            parents=[Compare.get_parser()],
        ).set_defaults(
            func=Compare,
        )

    @staticmethod
    def factory(argv):
        return Compare(Compare.get_parser().parse_args(argv))

    def compare(self):
        a = self.origin
        self.origin_d = collections.defaultdict(lambda: 0)
        b = self.destination
        self.destination_d = collections.defaultdict(lambda: 0)
        replication_count = self.args.replication_count
        rule = self.args.rule
        self.from_to = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        for value in range(0, self.args.values_count):
            am = a.map(rule, value, replication_count)
            assert len(am) == replication_count
            for d in am:
                self.origin_d[d] += 1
            bm = b.map(rule, value, replication_count)
            assert len(bm) == replication_count
            for d in bm:
                self.destination_d[d] += 1
            if self.args.order_matters:
                for i in range(len(am)):
                    if am[i] != bm[i]:
                        self.from_to[am[i]][bm[i]] += 1
            else:
                am = set(am)
                bm = set(bm)
                if am == bm:
                    continue
                ar = list(am - bm)
                br = list(bm - am)
                for i in range(len(ar)):
                    self.from_to[ar[i]][br[i]] += 1
        return self.from_to

    def display(self):
        out = ""
        o = pd.Series(self.origin_d)
        objects_count = o.sum()
        out += "There are {} objects.\n".format(objects_count)
        m = pd.DataFrame.from_dict(self.from_to, dtype=int).fillna(0).T.astype(int)
        objects_moved = m.sum().sum()
        objects_moved_percent = objects_moved / float(objects_count) * 100
        out += textwrap.dedent("""
        Replacing the crushmap specified with --origin with the crushmap
        specified with --destination will move {} objects ({}% of the total)
        from one item to another.
        """.format(int(objects_moved), objects_moved_percent))
        from_to_percent = m.sum(axis=1) / objects_count
        to_from_percent = m.sum() / objects_count
        m['objects%'] = from_to_percent.apply(lambda v: "{:.2%}".format(v))
        mt = m.T
        mt['objects%'] = to_from_percent.apply(lambda v: "{:.2%}".format(v))
        m = mt.T.fillna("{:.2f}%".format(objects_moved_percent))
        out += textwrap.dedent("""
        The rows below show the number of objects moved from the given
        item to each item named in the columns. The objects% at the
        end of the rows shows shows the percentage of the total number
        of objects that is moved away from this particular item. The
        last row shows the percentage of the total number of objects
        that is moved to the item named in the column.

        """)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 160)
        out += str(m)
        return out

    def run(self):
        self.run_compare()
        print(self.display())

    def run_compare(self):
        o = Crush(verbose=self.args.verbose,
                  backward_compatibility=self.args.backward_compatibility)
        o.parse(json.load(open(self.args.origin)))
        self.set_origin(o)
        d = Crush(verbose=self.args.verbose,
                  backward_compatibility=self.args.backward_compatibility)
        d.parse(json.load(open(self.args.destination)))
        self.set_destination(d)
        self.compare()

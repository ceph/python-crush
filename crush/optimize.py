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
import logging
import textwrap

from crush import Crush
from crush import analyze
from crush import compare
from crush.analyze import Analyze, BadMapping
from multiprocessing import Pool

log = logging.getLogger(__name__)


def top_optimize(args):
    self = args[0]
    return self.optimize_bucket(*args[1:])


class Optimize(object):

    def __init__(self, args, main):
        self.args = args
        self.main = main

    def __getstate__(self):
        return (self.main,)

    def __setstate__(self, state):
        self.main = state[0]
        self.args = self.main.args

    @staticmethod
    def get_parser():
        parser = Analyze.get_parser()
        parser.add_argument(
            '--step',
            help='optimization steps (default infinite)',
            type=int,
        )
        parser.add_argument(
            '--no-positions',
            dest='with_positions',
            action='store_false', default=True,
            help='optimize weiths for each position (default: true)')
        parser.add_argument(
            '--no-multithread',
            dest='multithread',
            action='store_false', default=True,
            help='use multithread when possible (default: true)')
        parser.add_argument(
            '--out-path',
            help='path of the output file')
        return parser

    @staticmethod
    def set_parser(subparsers, arguments):
        parser = Optimize.get_parser()
        arguments(parser)
        subparsers.add_parser(
            'optimize',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent("""\
            Optimize a crushmap rule

            A rule may not produce the expected distribution if:

            - there are not enough values

            - the weights are not all the same and the replication count is
              two or more

            The optimization of a crush rule consists of modifying the
            weights of each item to get closer to the expected
            distribution.

            A simulation is run with values (--values-count) using the
            rule (--rule) from the crushmap (--crushmap). If the
            observed distribution is different from the expected
            distribution, alternative weights are calculated and
            stored under the --choose-args name. The resulting
            crushmap, including the optimized weights, is saved
            in the --out-path file.

            The optimization runs in parallel, unless the
            --no-multithread flag is given.

            By default weights are optimized for each replication
            level in the range [1,--replication-count]. If the
            --no-positions flag is given, a single set of weights is
            produce and can be used as replacements in crushmaps that
            cannot handle multiple weights per item.

            Optimization is an iterative process that can be stopped
            at any time and resumed later. If the --step flag is not
            specified, the optimization runs until it cannot get
            closer to the desired distribution. If specified, --step
            stops optimization if it moves more than the specified
            number of values. It is useful to optimize a crushmap
            while controling data movement. For instance --step 1
            stops after the optimization step that moves one value or
            more.

            """),
            epilog=textwrap.dedent("""
            Examples:

            crush optimize \\
                  --choose-args optimize --rule replicated_ruleset \\
                  --replication-count 3 --crushmap map.json \\
                  --out-path optimized.json
            2017-05-15 11:15:01,256 INFO default done, replica 1
            2017-05-15 11:15:13,363 INFO default done, replica 2
            2017-05-15 11:15:16,488 INFO default done, replica 3
            2017-05-15 11:15:18,959 INFO cloud6-1433 done, replica 1
            2017-05-15 11:15:19,977 INFO cloud6-1429 done, replica 1
            ...
            """),
            help='Optimize crushmaps',
            parents=[parser],
        ).set_defaults(
            func=Optimize,
        )

    def get_choose_arg(self, crushmap, bucket):
        if 'choose_args' not in crushmap:
            crushmap['choose_args'] = {}
        if self.args.choose_args not in crushmap['choose_args']:
            crushmap['choose_args'][self.args.choose_args] = []
        choose_arg = None
        for element in crushmap['choose_args'][self.args.choose_args]:
            if element['bucket_id'] == bucket['id']:
                choose_arg = element
                break
        if choose_arg is None:
            choose_arg = {'bucket_id': bucket['id']}
            crushmap['choose_args'][self.args.choose_args].append(choose_arg)
        return choose_arg

    def set_choose_arg_position(self, choose_arg, bucket, choose_arg_position):
        if 'weight_set' not in choose_arg:
            choose_arg['weight_set'] = []
        if len(choose_arg['weight_set']) == 0:
            id2weight = collections.OrderedDict(
                [(i['id'], i['weight']) for i in bucket['children']])
            choose_arg['weight_set'] = [list(id2weight.values())]
        if len(choose_arg['weight_set']) <= choose_arg_position:
            last = choose_arg['weight_set'][-1]
            choose_arg['weight_set'].extend(
                [last] * (choose_arg_position - len(choose_arg['weight_set']) + 1))

    def optimize_bucket(self, p, origin_crushmap, bucket):
        if len(bucket.get('children', [])) == 0:
            return None
        log.warning(bucket['name'] + " optimizing")
        crushmap = copy.deepcopy(origin_crushmap)
        if self.args.with_positions:
            for replication_count in range(1, self.args.replication_count + 1):
                log.debug(bucket['name'] + " improving replica " + str(replication_count))
                count = self.optimize_replica(p, origin_crushmap,
                                              crushmap, bucket,
                                              replication_count, replication_count - 1)
        else:
            count = self.optimize_replica(p, origin_crushmap,
                                          crushmap, bucket,
                                          self.args.replication_count, 0)
        return (count, self.get_choose_arg(crushmap, bucket))

    def optimize_replica(self, p, origin_crushmap,
                         crushmap, bucket,
                         replication_count, choose_arg_position):
        a = self.main.clone().constructor(['analyze'] + p)
        a.args.replication_count = replication_count

        parser = compare.Compare.get_parser()
        self.main.hook_compare_args(parser)
        cp = self.main.get_trimmed_argv(parser, self.args)
        compare_instance = self.main.clone().constructor(['compare'] + cp)
        compare_instance.args.replication_count = replication_count
        compare_instance.set_origin_crushmap(origin_crushmap)

        choose_arg = self.get_choose_arg(crushmap, bucket)
        self.set_choose_arg_position(choose_arg, bucket, choose_arg_position)
        id2weight = collections.OrderedDict()
        for pos in range(len(bucket['children'])):
            v = choose_arg['weight_set'][choose_arg_position][pos]
            id2weight[bucket['children'][pos]['id']] = v

        log.info(bucket['name'] + " optimizing replica " + str(replication_count) + " " +
                 str(list(id2weight.values())))
        c = Crush(backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)

        (take, failure_domain) = c.rule_get_take_failure_domain(a.args.rule)
        #
        # initial simulation
        #
        i = a.run_simulation(c, take, failure_domain)
        i = i.reset_index()
        s = i['~name~'] == 'KKKK'  # init to False, there must be a better way
        for item in bucket['children']:
            s |= (i['~name~'] == item['name'])

        previous_delta = None
        improve_tolerance = 10
        no_improvement = 0
        max_iterations = 1000
        from_to_count = 0
        for iterations in range(max_iterations):
            previous_weights = choose_arg['weight_set'][choose_arg_position]
            choose_arg['weight_set'][choose_arg_position] = list(id2weight.values())
            c.parse(crushmap)
            try:
                z = a.run_simulation(c, take, failure_domain)
            except BadMapping:
                log.error("stop, got one bad mapping with " + str(id2weight.values()))
                choose_arg['weight_set'][choose_arg_position] = previous_weights
                break
            z = z.reset_index()
            d = z[s].copy()
            d['~delta~'] = d['~objects~'] - d['~expected~']
            d['~delta%~'] = d['~delta~'] / d['~expected~']
            delta = d['~delta~'].abs().sum()
            if previous_delta is not None:
                if previous_delta < delta:
                    no_improvement += 1
                else:
                    previous_delta = delta
                    best_weights = list(id2weight.values())
                    no_improvement = 0
                if no_improvement >= improve_tolerance:
                    choose_arg['weight_set'][choose_arg_position] = best_weights
                    break
            else:
                best_weights = list(id2weight.values())
                previous_delta = delta
            log.debug(bucket['name'] + " delta " + str(delta) +
                      " no_improvement " + str(no_improvement))
            d = d.sort_values('~delta~', ascending=False)
            if d.iloc[0]['~delta~'] <= 0 or d.iloc[-1]['~delta~'] >= 0:
                break
            if self.args.step and no_improvement == 0:
                compare_instance.set_destination(c)
                (from_to, in_out) = compare_instance.compare_bucket(bucket)
                from_to_count = sum(map(lambda x: sum(x.values()), from_to.values()))
                in_out_count = sum(map(lambda x: sum(x.values()), in_out.values()))
                log.debug("moved from_to " + str(from_to_count) +
                          " in_out " + str(in_out_count))
                if from_to_count > self.args.step:
                    log.debug("stopped because moved " + str(from_to_count) +
                              " --step " + str(self.args.step))
                    break
            # there should not be a need to keep the sum of the weights to the same value, they
            # are only used locally for placement and have no impact on the upper weights
            # nor are they derived from the weights from below *HOWEVER* in case of a failure
            # the weights need to be as close as possible from the target weight to limit
            # the negative impact
            shift = id2weight[d.iloc[0]['~id~']] * min(0.01, d.iloc[0]['~delta%~'])
            if id2weight[d.iloc[-1]['~id~']] < shift:
                break
            id2weight[d.iloc[0]['~id~']] -= shift
            id2weight[d.iloc[-1]['~id~']] += shift
        if iterations >= max_iterations - 1:
            log.debug("stopped after " + str(iterations))
        log.warning(bucket['name'] + " replica " + str(replication_count) + " optimized")
        log.info(bucket['name'] + " weights " + str(list(id2weight.values())))
        return from_to_count

    def optimize(self, crushmap):
        c = Crush(backward_compatibility=self.args.backward_compatibility)
        c.parse(crushmap)
        crushmap = c.get_crushmap()
        if 'choose_args' not in crushmap:
            crushmap['choose_args'] = {}
            c.parse(crushmap)
        if self.args.choose_args not in crushmap['choose_args']:
            crushmap['choose_args'][self.args.choose_args] = []
            c.parse(crushmap)
        (take, failure_domain) = c.rule_get_take_failure_domain(self.args.rule)

        parser = analyze.Analyze.get_parser()
        self.main.hook_analyze_args(parser)
        p = self.main.get_trimmed_argv(parser, self.args)
        a = self.main.clone().constructor(['analyze'] + p)

        if self.args.multithread:
            pool = Pool()
        children = [c.find_bucket(take)]
        total_count = 0
        while len(children) > 0:
            a = [(self, p, c.get_crushmap(), item) for item in children]
            if self.args.multithread:
                r = list(filter(None, pool.map(top_optimize, a)))
            else:
                r = list(filter(None, map(top_optimize, a)))
            if r:
                total_count += sum([x[0] for x in r])
                choose_args = [x[1] for x in r]
                c.update_choose_args(self.args.choose_args, choose_args)
                if self.args.step and total_count > self.args.step:
                    break
            nc = []
            for item in children:
                nc.extend(item.get('children', []))
            # fail if all children are not of the same type
            children = nc
        return (total_count, c.get_crushmap())

    def run(self):
        if not self.args.crushmap:
            raise Exception("missing --crushmap")
        crushmap = self.main.convert_to_crushmap(self.args.crushmap)
        if not self.args.choose_args:
            raise Exception("missing --choose-args")
        (count, crushmap) = self.optimize(crushmap)
        self.main.crushmap_to_file(crushmap)

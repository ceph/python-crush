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
from crush.analyze import Analyze

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
            '--no-forecast',
            dest='with_forecast',
            action='store_false', default=True,
            help='how many steps to completion (default: true)')
        parser.add_argument(
            '--no-positions',
            dest='with_positions',
            action='store_false', default=True,
            help='optimize weigths for each position (default: true)')
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
            and resumed later. If the --step flag is not specified,
            the optimization runs until it cannot get closer to the
            desired distribution. If specified, the value of the
            --step flag is the number of items that are moved by
            changing the bucket weights. The optimization will stop if
            more than --step items are moved. For instance --step 1
            stops after the optimization step that moves one value or
            more.

            When --step is specified, the crushmap for the first step
            is written to --out-path and the optimization process
            continues to show how many steps remain before the rule
            cannot be optimized any more. The --no-forecast flag
            forces optimization to stop right after the first step.
            """),
            epilog=textwrap.dedent("""
            Examples:

            crush optimize \\
                  --choose-args optimize --rule replicated_ruleset \\
                  --replication-count 3 --crushmap map.json \\
                  --out-path optimized.json
            2017-05-24 11:12:34,752 default optimizing
            2017-05-24 11:12:39,361 default wants to swap 10 objects
            2017-05-24 11:12:39,362 default will swap 10 objects
            2017-05-24 11:12:39,369 cloud3-1359 optimizing
            2017-05-24 11:12:39,370 cloud3-1360 optimizing
            ...
            """),
            help='Optimize crushmaps',
            parents=[parser],
        ).set_defaults(
            func=Optimize,
        )

    def pre_sanity_check_args(self):
        self.main.hook_optimize_pre_sanity_check_args(self.args)

    def post_sanity_check_args(self):
        self.main.hook_optimize_post_sanity_check_args(self.args)

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
        if bucket.get('algorithm', 'straw2') != 'straw2':
            raise ValueError(bucket['name'] + ' algorithm is ' + bucket['algorithm'] +
                             ', only straw2 can be optimized')
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
        if count > 0:
            n = self.main.value_name()
            log.warning(bucket['name'] + " wants to swap " + str(count) + " " + n)
        else:
            log.warning(bucket['name'] + " already optimized")
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
        log.debug(bucket['name'] + " optimizing replica " + str(replication_count) + " " +
                  str(dict(id2weight)))
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
        best_weights = list(id2weight.values())
        n = self.main.value_name()
        for iterations in range(max_iterations):
            choose_arg['weight_set'][choose_arg_position] = list(id2weight.values())
            c.parse(crushmap)
            z = a.run_simulation(c, take, failure_domain)
            z = z.reset_index()
            d = z[s].copy()
            d['~delta~'] = d['~' + n + '~'] - d['~expected~']
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
                    log.info("stop because " + str(no_improvement) + " tries")
                    break
            else:
                best_weights = list(id2weight.values())
                previous_delta = delta
            if delta == 0:
                log.info("stop because the distribution is perfect")
                break
            log.info(bucket['name'] + " delta " + str(delta))
            if self.args.step and no_improvement == 0:
                compare_instance.set_destination(c)
                (from_to, in_out) = compare_instance.compare_bucket(bucket)
                from_to_count = sum(map(lambda x: sum(x.values()), from_to.values()))
                in_out_count = sum(map(lambda x: sum(x.values()), in_out.values()))
                log.debug("moved from_to " + str(from_to_count) +
                          " in_out " + str(in_out_count))
                if from_to_count > self.args.step:
                    log.info("stopped because moved " + str(from_to_count) +
                             " --step " + str(self.args.step))
                    break
            d = d.sort_values('~delta~', ascending=False)
            if d.iloc[0]['~delta~'] <= 0 or d.iloc[-1]['~delta~'] >= 0:
                log.info("stop because [" + str(d.iloc[0]['~delta~']) + "," +
                         str(d.iloc[-1]['~delta~']) + "]")
                break
            # there should not be a need to keep the sum of the weights to the same value, they
            # are only used locally for placement and have no impact on the upper weights
            # nor are they derived from the weights from below *HOWEVER* in case of a failure
            # the weights need to be as close as possible from the target weight to limit
            # the negative impact
            shift = int(id2weight[d.iloc[0]['~id~']] * min(0.01, abs(d.iloc[0]['~delta%~'])))
            if shift <= 0:
                log.info("stop because shift is zero")
                break
            log.debug("shift from " + str(d.iloc[0]['~id~']) +
                      " to " + str(d.iloc[-1]['~id~']))
            id2weight[d.iloc[0]['~id~']] -= shift
            id2weight[d.iloc[-1]['~id~']] += shift

        choose_arg['weight_set'][choose_arg_position] = best_weights
        c.parse(crushmap)
        compare_instance.set_destination(c)
        (from_to, in_out) = compare_instance.compare_bucket(bucket)
        from_to_count = sum(map(lambda x: sum(x.values()), from_to.values()))

        if iterations >= max_iterations - 1:
            log.info("stopped after " + str(iterations))
        log.info(bucket['name'] + " replica " + str(replication_count) + " optimized")
        log.info(bucket['name'] + " weights " + str(choose_arg['weight_set'][choose_arg_position]))
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
            from multiprocessing import Pool
            pool = Pool()
        children = [c.find_bucket(take)]
        total_count = 0
        over_step = False
        n = self.main.value_name()
        while not over_step and len(children) > 0:
            a = [(self, p, c.get_crushmap(), item) for item in children]
            if self.args.multithread:
                r = list(pool.map(top_optimize, a))
            else:
                r = list(map(top_optimize, a))
            for i in range(len(children)):
                if r[i] is None:
                    continue
                (count, choose_arg) = r[i]
                total_count += count
                c.update_choose_args(self.args.choose_args, [choose_arg])
                log.info(children[i]['name'] + " weights updated with " + str(choose_arg))
                if self.args.step and count > 0:
                    log.warning(children[i]['name'] + " will swap " +
                                str(count) + " " + n)
                over_step = self.args.step and total_count > self.args.step
                if over_step:
                    break
            nc = []
            for item in children:
                nc.extend(item.get('children', []))
            # fail if all children are not of the same type
            children = nc
        return (total_count, c.get_crushmap())

    def run(self):
        self.pre_sanity_check_args()
        crushmap = self.main.convert_to_crushmap(self.args.crushmap)
        self.post_sanity_check_args()
        (count, crushmap) = self.optimize(crushmap)
        self.main.crushmap_to_file(crushmap)
        if self.args.step and self.args.with_forecast:
            log.warning("the optimized crushmap was written to " + self.args.out_path)
            log.warning("now running simulation of the next steps")
            log.warning("this can be disabled with --no-forecast")
            step = 2
            n = self.main.value_name()
            while count > 0:
                (count, crushmap) = self.optimize(crushmap)
                log.warning("step " + str(step) + " moves " + str(count) + " " + n)
                step += 1

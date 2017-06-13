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
import copy
import logging
import pytest # noqa needed for capsys
import os
import pickle

from crush import Crush
from crush.ceph import Ceph
from crush.main import Main
from crush.analyze import BadMapping

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.INFO)


class TestOptimize(object):

    def test_sanity_check_args(self):
        a = Main().constructor([
            'optimize',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --crushmap' in str(e.value)

        a = Main().constructor([
            'optimize',
            '--crushmap', 'CRUSHMAP',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --out-path' in str(e.value)

        a = Main().constructor([
            'optimize',
            '--crushmap', 'CRUSHMAP',
            '--out-path', 'OUT PATH',
            '--no-forecast',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'only valid with --step' in str(e.value)

        a = Main().constructor([
            'optimize',
            '--crushmap', 'CRUSHMAP',
            '--out-path', 'OUT PATH',
        ])
        a.pre_sanity_check_args()
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'missing --rule' in str(e.value)

        a = Main().constructor([
            'optimize',
            '--crushmap', 'CRUSHMAP',
            '--out-path', 'OUT PATH',
            '--rule', 'RULE',
        ])
        a.pre_sanity_check_args()
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'missing --choose-args' in str(e.value)

        a = Main().constructor([
            'optimize',
            '--crushmap', 'CRUSHMAP',
            '--out-path', 'OUT PATH',
            '--rule', 'RULE',
            '--choose-args', 'CHOOSE ARGS',
        ])
        a.pre_sanity_check_args()
        a.post_sanity_check_args()

    def test_pickle(self):
        o = Ceph().constructor(['optimize'])
        p = pickle.dumps(o)
        oo = pickle.loads(p)
        assert oo.main.argv == o.main.argv
        assert type(oo) == type(o)

    def test_set_optimize_args(self):
        a = Ceph().constructor([
            'optimize',
            '--pool', '3',
        ])
        a.args.replication_count = None
        assert a.args.choose_args is None
        assert a.args.rule is None
        assert a.args.pg_num is None
        assert a.args.pgp_num is None
        assert a.args.out_version == 'luminous'
        assert a.args.with_positions is True
        a.main.convert_to_crushmap('tests/ceph/ceph-report-small.json')
        assert a.args.replication_count == 3
        assert a.args.choose_args == '3'
        assert a.args.rule == 'data'
        assert a.args.pg_num == 1
        assert a.args.pgp_num == 1
        assert a.args.out_version == 'j'
        assert a.args.with_positions is False

    def test_get_choose_arg(self):
        a = Ceph().constructor([
            'optimize',
            '--choose-args', 'optimize',
        ])
        crushmap = {}

        bucket = {'id': -1}

        choose_arg = a.get_choose_arg(crushmap, bucket)
        assert {'bucket_id': -1} == choose_arg
        assert [choose_arg] == crushmap['choose_args']['optimize']

        choose_arg = a.get_choose_arg(crushmap, bucket)
        assert {'bucket_id': -1} == choose_arg
        assert [choose_arg] == crushmap['choose_args']['optimize']

        bucket = {'id': -2}

        choose_arg = a.get_choose_arg(crushmap, bucket)
        assert [{'bucket_id': -1}, {'bucket_id': -2}] == crushmap['choose_args']['optimize']

    def test_set_choose_arg_position(self):
        a = Ceph().constructor([
            'optimize',
            '--choose-args', 'optimize',
        ])
        bucket = {
            'id': -1,
            'children': [
                {'id': 1, 'weight': 10},
                {'id': 2, 'weight': 20},
            ],
        }
        choose_arg = {'bucket_id': -1}
        a.set_choose_arg_position(choose_arg, bucket, 0)
        assert {'bucket_id': -1, 'weight_set': [[10, 20]]} == choose_arg

        a.set_choose_arg_position(choose_arg, bucket, 1)
        assert {'bucket_id': -1, 'weight_set': [[10, 20], [10, 20]]} == choose_arg

        choose_arg['weight_set'][1] = [100, 200]
        a.set_choose_arg_position(choose_arg, bucket, 3)
        expected = {'bucket_id': -1, 'weight_set': [
            [10, 20], [100, 200], [100, 200], [100, 200]
        ]}
        assert expected == choose_arg

        a.set_choose_arg_position(choose_arg, bucket, 1)
        assert expected == choose_arg

    def make_bucket(self, weights):
        crushmap = {
            "choose_args": {"optimize": []},
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(weights),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", 0],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "id": i,
                "name": "device%d" % i,
                "weight": weights[i] * 0x10000,
            } for i in range(len(weights))
        ])
        return crushmap

    def verify_optimize(self, weights, expected_delta,
                        values_count, replication_count):
        crushmap = self.make_bucket(weights)
        rule = 'firstn'
        p = [
            '--values-count', str(values_count),
            '--replication-count', str(replication_count),
            '--rule', rule,
            '--choose-args', 'optimize',
        ]
        o = Main().constructor(['--verbose', 'optimize'] + p)
        origin_crushmap = copy.deepcopy(crushmap)
        bucket = crushmap['trees'][0]
        previous_weight_set = []
        for position in range(replication_count):
            o.optimize_replica(
                p, origin_crushmap, crushmap, bucket, replication_count, position)
            assert 1 == len(crushmap['choose_args']['optimize'])
            weight_set = copy.deepcopy(crushmap['choose_args']['optimize'][0]['weight_set'])
            assert weight_set[:len(previous_weight_set)] == previous_weight_set

        a = Main().constructor(['analyze', '--choose-args', 'optimize'] + p)
        r = a.analyze_crushmap(crushmap)
        delta = (r['~objects~'] - r['~expected~']).tolist()
        print("delta = " + str(delta))
        assert expected_delta == delta

    def test_simple(self):
        # values_count=100 is not enough samples
        self.verify_optimize([1, 1, 1], [0, 0, 0, 0],
                             values_count=100, replication_count=2)

        self.verify_optimize([5, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                             [0, -3, 0, 1, 0, 0, 0, 0, 0, 1, 1],
                             values_count=100, replication_count=6)

        self.verify_optimize([1, 2, 3, 1, 2, 3, 1, 2, 3, 1],
                             [0, 0, 0, 0, -1, 0, 0, 0, 1, 0, 0],
                             values_count=100, replication_count=6)

    def test_very_different_weights(self):
        # values_count=10000 is not enough samples
        delta = [0, -1, -1, -1, -1, 1, -1, -1, -1, 0, -1, 1, 0, -1, -1, 1, 2, 1, 2, 1, 0, 0, 1,
                 -1, -1, -1, 0, 0, 1, 1, 0, -1, 0, 0, 1, 1, -1, -1, -1, -1, 0, 0, 1, 1, -1, 0,
                 0, 1, 0, -1, -1, 0, 2, 1, 0, -1, 1, 0, 0, -1, 0, 0, 2, -1, 1, 1, 1, -1, 0, 1,
                 0, -1, 0, 0, 1, -1, 1, -1, 0, 1, 0, 0, 1, -1, 1, -1, 1, -1, -1, 1, -1, 1, 0,
                 -1, 0, 0, 0, 0, 0, -1, 0]
        self.verify_optimize(range(1, 101), delta,
                             values_count=10000, replication_count=1)

    def test_overweighted(self):
        # 5 is overweighted for replica > 1 because
        self.verify_optimize([5, 1, 1, 1, 1], [0, 0, 0, 0, 0, 0],
                             values_count=100, replication_count=1)
        self.verify_optimize([5, 1, 1, 1, 1], [0, -3, 0, 0, 1, 2],
                             values_count=100, replication_count=2)

    @pytest.mark.skipif(os.environ.get('LONG') is None, reason="LONG")
    def test_probability_bias(self):
        self.verify_optimize([5, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                             [0, 0, 0, 0, 0, 0, -1, -1, 1, 0, 1],
                             replication_count=2,
                             values_count=100000)

    def test_bad_mapping(self):
        with pytest.raises(BadMapping):
            self.verify_optimize([1000, 1, 1, 1, 1], [],
                                 replication_count=2, values_count=100)

    def run_optimize(self, p, crushmap, gain):
        o = Ceph().constructor(['optimize', '--choose-args', 'optimize'] + p)
        origin_crushmap = copy.deepcopy(crushmap)
        (count, optimized) = o.optimize(crushmap)
        self.analyze_optimization(p, origin_crushmap, optimized, gain)

    def analyze_optimization(self, p, crushmap, optimized, gain):
        a = Ceph().constructor(['analyze'] + p)
        before = a.analyze_crushmap(crushmap)
        print("============= before")
        print(str(before))

        a = Ceph().constructor(['analyze', '--choose-args', 'optimize'] + p)
        after = a.analyze_crushmap(optimized)
        print("============= after")
        print(str(after))

        for type in before['~type~'].unique():
            b = before.loc[before['~type~'] == type]
            b = b.sort_values(by='~over/under filled %~', ascending=False)
            b_span = b.iloc[0]['~over/under filled %~'] - b.iloc[-1]['~over/under filled %~']
            a = after.loc[after['~type~'] == type]
            a = a.sort_values(by='~over/under filled %~', ascending=False)
            a_span = a.iloc[0]['~over/under filled %~'] - a.iloc[-1]['~over/under filled %~']
            print("============= span " + str(type) + " before " +
                  str(b_span) + " after " + str(a_span))
            assert a_span <= b_span / gain

    def test_optimize_one_step(self):
        pg_num = 2048
        size = 3
        a = Ceph().constructor([
            'optimize',
            '--no-multithread',
            '--replication-count', str(size),
            '--pool', '3',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'data',
            '--choose-args', 'optimize',
            '--step', '64',
        ])
        c = Crush(backward_compatibility=True)
        c.parse('tests/test_optimize_small_cluster.json')
        crushmap = c.get_crushmap()
        (count, crushmap) = a.optimize(crushmap)
        assert 240 == count

    def test_optimize_report_compat_one_pool(self):
        #
        # verify --choose-args is set to --pool when the crushmap contains
        # *-target-weights buckets.
        #
        expected_path = 'tests/ceph/ceph-report-compat-optimized.txt'
        out_path = expected_path + ".err"
        for p in ([],
                  ['--choose-args=3'],
                  ['--pool=3'],
                  ['--choose-args=3', '--pool=3']):
            Ceph().main([
                '--verbose',
                'optimize',
                '--no-multithread',
                '--crushmap', 'tests/ceph/ceph-report-compat.json',
                '--out-path', out_path,
                '--out-format', 'txt',
            ] + p)
            assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
            os.unlink(out_path)

    def test_optimize_report_compat_two_pools(self):
        expected_path = 'tests/ceph/ceph-report-compat-optimized.txt'
        out_path = expected_path + ".err"
        for p in (['--pool=3'],
                  ['--choose-args=3', '--pool=3']):
            Ceph().main([
                '--verbose',
                'optimize',
                '--no-multithread',
                '--crushmap', 'tests/ceph/ceph-report-compat.json',
                '--out-path', out_path,
                '--out-format', 'txt',
            ] + p)
            assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
            os.unlink(out_path)

        with pytest.raises(Exception) as e:
            Ceph().main([
                '--verbose',
                'optimize',
                '--no-multithread',
                '--crushmap', 'tests/ceph/ceph-report-compat-two-pools.json',
                '--out-path', out_path,
                '--out-format', 'txt',
            ])
        assert '--pool is required' in str(e.value)

        with pytest.raises(Exception) as e:
            Ceph().main([
                '--verbose',
                'optimize',
                '--no-multithread',
                '--crushmap', 'tests/ceph/ceph-report-compat-two-pools.json',
                '--out-path', out_path,
                '--out-format', 'txt',
                '--pool', '1324',
            ])
        assert '1324 is not a known pool' in str(e.value)

    @pytest.mark.skipif(os.environ.get('LONG') is None, reason="LONG")
    def test_optimize_small_cluster(self):
        pg_num = 4096
        size = 3
        p = [
            '--replication-count', str(size),
            '--pool', '3',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'data',
        ]
        self.run_optimize(p, 'tests/test_optimize_small_cluster.json', 10)

    @pytest.mark.skipif(os.environ.get('LONG') is None, reason="LONG")
    def test_optimize_big_cluster(self):
        # few samples
        pg_num = 2048
        size = 3
        p = [
            '--replication-count', str(size),
            '--pool', '5',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'data',
        ]
        self.run_optimize(p, 'tests/test_optimize_big_cluster.json', 4)

    @pytest.mark.skipif(os.environ.get('LONG') is None, reason="LONG")
    def test_optimize_step(self):
        pg_num = 2048
        size = 3
        a = Ceph().constructor([
            'optimize',
            '--no-multithread',
            '--replication-count', str(size),
            '--pool', '3',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'data',
            '--choose-args', 'optimize',
            '--step', '64',
        ])
        c = Crush(backward_compatibility=True)
        c.parse('tests/test_optimize_small_cluster.json')
        crushmap = c.get_crushmap()
        converged = False
        for i in range(20):
            (count, crushmap) = a.optimize(crushmap)
            if count <= 0:
                converged = True
                break
            print("moved " + str(count) + " values")
        assert converged

    @pytest.mark.skipif(os.environ.get('LONG') is None, reason="LONG")
    def test_optimize_step_forecast(self, caplog):
        expected_path = 'tests/test_optimize_small_cluster_step_1.txt'
        out_path = expected_path + ".err"
        # few samples
        pg_num = 2048
        size = 3
        Ceph().main([
            '--verbose',
            'optimize',
            '--no-multithread',
            '--crushmap', 'tests/test_optimize_small_cluster.json',
            '--out-path', out_path,
            '--out-format', 'txt',
            '--replication-count', str(size),
            '--pool', '2',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'data',
            '--choose-args', '0',
            '--step', '64',
        ])

        assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
        os.unlink(out_path)
        assert 'step 2 moves 73 objects' in caplog.text()
        assert 'step 3 moves 76 objects' in caplog.text()
        assert 'step 4 moves 93 objects' in caplog.text()
        assert 'step 5 moves 80 objects' in caplog.text()
        assert 'step 6 moves 100 objects' in caplog.text()
        assert 'step 7 moves 80 objects' in caplog.text()
        assert 'step 8 moves 53 objects' in caplog.text()
        assert 'step 9 moves 0 objects' in caplog.text()

# Local Variables:
# compile-command: "cd .. ; LONG=yes tox -e py27 -- -s -vv tests/test_optimize.py"
# End:

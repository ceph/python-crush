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
import logging
import pytest # noqa needed for capsys
import os
import pprint
import pickle

from crush import Crush
from crush.ceph import Ceph

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.INFO)


class TestOptimize(object):

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

    def run_optimize(self, p, crushmap, gain):
        o = Ceph().constructor(['optimize', '--choose-args', 'optimize'] + p)
        a = Ceph().constructor(['analyze'] + p)
        before = a.analyze_crushmap(crushmap)
        (count, optimized) = o.optimize(crushmap)
        pprint.pprint(optimized)
        a = Ceph().constructor(['analyze', '--choose-args', 'optimize'] + p)
        after = a.analyze_crushmap(optimized)
        print("============= before")
        print(str(before))
        print("============= after")
        print(str(after))

        for type in before['~type~'].unique():
            b = before.loc[before['~type~'] == type]
            b = b.sort_values(by='~over/under used %~', ascending=False)
            b_span = b.iloc[0]['~over/under used %~'] - b.iloc[-1]['~over/under used %~']
            a = after.loc[after['~type~'] == type]
            a = a.sort_values(by='~over/under used %~', ascending=False)
            a_span = a.iloc[0]['~over/under used %~'] - a.iloc[-1]['~over/under used %~']
            print("============= span " + str(type) + " before " +
                  str(b_span) + " after " + str(a_span))
            assert a_span <= b_span / gain

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
    def test_overweighted(self):
        # [ 5 1 1 1 1]
        size = 2
        pg_num = 2048

        hosts_count = 5
        host_weight = [1 * 0x10000] * hosts_count
        host_weight[0] = 5 * 0x10000
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(host_weight),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "weight": host_weight[i],
                "children": [],
            } for i in range(0, hosts_count)
        ])
        a = Ceph().constructor([
            '--verbose',
            'optimize',
            '--no-multithread',
            '--replication-count', str(size),
            '--pool', '0',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'firstn',
            '--choose-args', 'optimize',
        ])
        a.optimize(crushmap)

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
    def test_optimize_probability_bias(self):
        # [ 5 1 1 1 1 1 1 1 1 1 ]
        # there are enough samples an the uneven distribution only comes
        # from the bias introduced by conditional probabilities
        size = 2
        pg_num = 51200
        hosts_count = 10
        host_weight = [1 * 0x10000] * hosts_count
        host_weight[0] = 5 * 0x10000
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(host_weight),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "weight": host_weight[i],
                "children": [],
            } for i in range(0, hosts_count)
        ])
        p = [
            '--replication-count', str(size),
            '--pool', '0',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'firstn',
        ]
        self.run_optimize(p, crushmap, 10)

    def test_optimize_1(self):
        # [ 5 1 1 1 1 1 1 1 1 1 ]
        # few samples
        size = 2
        pg_num = 512
        hosts_count = 10
        host_weight = [1 * 0x10000] * hosts_count
        host_weight[0] = 5 * 0x10000
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(host_weight),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "weight": host_weight[i],
                "children": [],
            } for i in range(0, hosts_count)
        ])
        p = [
            '--replication-count', str(size),
            '--pool', '0',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'firstn',
        ]
        self.run_optimize(p, crushmap, 10)

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
    def test_optimize_2(self):
        # [ 1 2 3 4 5 6 7 8 9 10 ... 100 ]
        # few samples
        size = 2
        hosts_count = 100
        pg_num = hosts_count * 200
        host_weight = [i * 0x10000 for i in range(1, hosts_count + 1)]
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(host_weight),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "weight": host_weight[i],
                "children": [],
            } for i in range(0, hosts_count)
        ])
        p = [
            '--replication-count', str(size),
            '--pool', '0',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'firstn',
        ]
        self.run_optimize(p, crushmap, 10)

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
    def test_optimize_3(self):
        # [ 1 2 3 1 2 3 1 2 3 1 ]
        # few samples
        size = 2
        pg_num = 512
        hosts_count = 10
        host_weight = [(i % 3 + 1) * 0x10000 for i in range(hosts_count)]
        crushmap = {
            "trees": [
                {
                    "type": "root",
                    "id": -1,
                    "name": "dc1",
                    "weight": sum(host_weight),
                    "children": [],
                }
            ],
            "rules": {
                "firstn": [
                    ["take", "dc1"],
                    ["set_choose_tries", 100],
                    ["choose", "firstn", 0, "type", "host"],
                    ["emit"]
                ],
            }
        }
        crushmap['trees'][0]['children'].extend([
            {
                "type": "host",
                "id": -(i + 2),
                "name": "host%d" % i,
                "weight": host_weight[i],
                "children": [],
            } for i in range(0, hosts_count)
        ])
        p = [
            '--replication-count', str(size),
            '--pool', '0',
            '--pg-num', str(pg_num),
            '--pgp-num', str(pg_num),
            '--rule', 'firstn',
        ]
        self.run_optimize(p, crushmap, 10)

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
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

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
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

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
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

    @pytest.mark.skipif(os.environ.get('ALL') is None, reason="ALL")
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
# compile-command: "cd .. ; ALL=yes tox -e py27 -- -s -vv tests/test_optimize.py"
# End:
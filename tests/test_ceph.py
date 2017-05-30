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
import os
import json
import pytest  # noqa needed for capsys

from crush.ceph import Ceph
from crush import ceph

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG)


class TestCephReport(object):

    def load(self):
        with open('tests/ceph/ceph-report-small.json') as f:
            return json.load(f)

    def test_good_by_default(self):
        ceph.CephReport().parse_report(self.load())

    def test_fail_health(self):
        report = self.load()
        report['health']['overall_status'] = 'HEALTH_WARN'
        with pytest.raises(ceph.HealthError):
            ceph.CephReport().parse_report(report)

    def test_fail_primary_affinity(self):
        report = self.load()
        report['osdmap']['osds'][0]['primary_affinity'] = 0.5
        with pytest.raises(ceph.UnsupportedError) as e:
            ceph.CephReport().parse_report(report)
        assert 'affinity is !=' in str(e.value)

    def test_fail_pool_type(self):
        report = self.load()
        report['osdmap']['pools'][0]['type'] = 2
        with pytest.raises(ceph.UnsupportedError) as e:
            ceph.CephReport().parse_report(report)
        assert 'only type == 1' in str(e.value)

    def test_fail_pool_object_hash(self):
        report = self.load()
        report['osdmap']['pools'][0]['object_hash'] = 5
        with pytest.raises(ceph.UnsupportedError) as e:
            ceph.CephReport().parse_report(report)
        assert 'only object_hash == 2' in str(e.value)

    def test_fail_pool_flags_names(self):
        report = self.load()
        report['osdmap']['pools'][0]['flags_names'] = 'somethingelse'
        with pytest.raises(ceph.UnsupportedError) as e:
            ceph.CephReport().parse_report(report)
        assert 'only hashpspool' in str(e.value)

    def test_fail_mapping_name(self, caplog):
        report = self.load()
        report['pgmap']['pg_stats'][0]['pgid'] = '5.1'
        with pytest.raises(ceph.MappingError):
            ceph.CephReport().parse_report(report)
        assert 'is not in pgmap' in caplog.text()

    def test_fail_mapping_osds(self, caplog):
        report = self.load()
        report['pgmap']['pg_stats'][0]['acting'] = [1, 2, 3]
        with pytest.raises(ceph.MappingError):
            ceph.CephReport().parse_report(report)
        assert 'instead of [1, 2, 3]' in caplog.text()


class TestCeph(object):

    def test_conversions(self):
        base = 'tests/sample-ceph-crushmap.'
        for ext_in in ('txt', 'crush', 'json', 'python-json'):
            in_path = base + ext_in
            for ext_out in ('txt', 'crush', 'json', 'python-json'):
                expected_path = base + ext_out
                out_path = expected_path + ".err"
                print("conversion " + in_path + " => " + expected_path)
                Ceph().main([
                    'convert',
                    '--in-path', in_path,
                    '--out-path', out_path,
                    '--out-format', ext_out,
                ])
                if ext_out == 'crush':
                    cmd = "cmp"
                else:
                    cmd = "diff -Bbu"
                assert os.system(cmd + " " + expected_path + " " + out_path) == 0
                os.unlink(out_path)

    def test_report(self):
        in_path = 'tests/ceph/ceph-report.json'
        expected_path = 'tests/ceph/crushmap-from-ceph-report.json'
        out_path = expected_path + ".err"
        Ceph().main([
            'convert',
            '--in-path', in_path,
            '--out-path', out_path,
            '--out-format', 'python-json',
        ])
        assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
        os.unlink(out_path)

    def test_report_compat(self):
        #
        # verify --choose-args is set to the pool when the crushmap contains
        # *-target-weights buckets.
        #
        expected_path = 'tests/ceph/ceph-report-compat-converted.txt'
        out_path = expected_path + ".err"
        Ceph().main([
            '--verbose',
            'convert',
            '--in-path', 'tests/ceph/ceph-report-compat.json',
            '--out-path', out_path,
            '--out-format', 'txt',
        ])
        assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
        os.unlink(out_path)

    def test_rules_order(self):
        expected_path = 'tests/ceph/ceph-crushmap-rules-order.txt'
        out_path = expected_path + ".err"
        Ceph().main([
            '--verbose',
            'convert',
            '--in-path', 'tests/ceph/ceph-crushmap-rules-order.json',
            '--out-path', out_path,
            '--out-format', 'txt',
        ])
        assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
        os.unlink(out_path)

    def test_hook_create_values(self):
        c = Ceph()
        c.parse([
            '--verbose',
            'analyze',
            '--values-count', '2',
        ])
        assert {0: 0, 1: 1} == c.hook_create_values()
        c.parse([
            '--verbose',
            'analyze',
            '--pool', '2',
            '--pg-num', '3',
            '--pgp-num', '3',
        ])
        expected = {u'2.0': -113899774, u'2.1': -1215435108, u'2.2': -832918304}
        assert expected == c.hook_create_values()

    def test_out_version(self):
        expected_path = 'tests/sample-ceph-crushmap-compat.txt'
        out_path = expected_path + ".err"

        in_path = 'tests/sample-ceph-crushmap-compat.python-json'
        Ceph().main([
            'convert',
            '--in-path', in_path,
            '--out-path', out_path,
            '--out-format', 'txt',
            '--out-version', 'jewel',
        ])
        assert os.system("diff -Bbu " + expected_path + " " + out_path) == 0
        os.unlink(out_path)

        in_path = 'tests/sample-ceph-crushmap.python-json'
        with pytest.raises(Exception) as e:
            Ceph().main([
                'convert',
                '--in-path', in_path,
                '--out-path', out_path,
                '--out-format', 'txt',
                '--out-version', 'jewel',
            ])
        assert 'version lower than luminous' in str(e.value)

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -vv -s tests/test_ceph.py"
# End:

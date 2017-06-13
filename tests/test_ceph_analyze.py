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
import pytest
from crush.ceph import Ceph


class TestAnalyze(object):

    def test_sanity_check_args(self):
        a = Ceph().constructor([
            'analyze',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --crushmap' in str(e.value)

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
        ])
        a.pre_sanity_check_args()

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
        ])
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'missing --rule' in str(e.value)

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
            '--rule', 'RULE',
        ])
        a.post_sanity_check_args()

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
            '--rule', 'RULE',
            '--pool', '3',
            '--values-count', '8',
        ])
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert '--pool and --values-count are mutually exclusive' in str(e.value)

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
            '--rule', 'RULE',
            '--pool', '3',
        ])
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert '--pg-num is required' in str(e.value)

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
            '--rule', 'RULE',
            '--pool', '3',
            '--pg-num', '10',
        ])
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert '--pgp-num is required' in str(e.value)

        a = Ceph().constructor([
            'analyze',
            '--crushmap', 'CRUSHMAP',
            '--rule', 'RULE',
            '--pool', '3',
            '--pg-num', '10',
            '--pgp-num', '10',
        ])
        a.post_sanity_check_args()

    def test_report_compat(self):
        #
        # verify --choose-args is set to the pool when the crushmap contains
        # *-target-weights buckets.
        #
        for p in ([],
                  ['--choose-args=3'],
                  ['--pool=3'],
                  ['--choose-args=3', '--pool=3']):
            a = Ceph().constructor([
                '--verbose',
                'analyze',
                '--crushmap', 'tests/ceph/ceph-report-compat.json',
            ] + p)
            d = a.analyze_report(*a.analyze())
            print(str(d))
            expected = """\
        ~id~  ~weight~  ~PGs~  ~over/under filled %~
~name~                                              
host0     -1       1.0      1                    0.0
host1     -2       1.0      1                    0.0
host2     -5       1.0      1                    0.0

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
device            33.33
host              33.33
rack               0.00
root               0.00\
""" # noqa trailing whitespaces are expected
            assert expected == str(d)

    def test_report_compat_hammer(self):
        #
        # hammer does not know about the stable tunable, verify this
        # is handled properly. It must be set to zero otherwise the
        # ceph report below will have unexpected mappings.
        #
        a = Ceph().constructor([
            '--verbose',
            'analyze',
            '--crushmap', 'tests/ceph/ceph-report-compat-hammer.json',
            '--pool', '42',
        ])
        d = a.analyze_report(*a.analyze())
        print(str(d))
        expected = """\
         ~id~  ~weight~  ~PGs~  ~over/under filled %~
~name~                                               
node-8v    -6      1.08      5                  56.25
node-5v    -3      1.08      4                  25.00
node-6v    -4      1.08      3                  -6.25
node-7v    -5      1.08      3                  -6.25
node-4     -2      1.08      1                 -68.75

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
device            150.0
host               75.0
root                0.0\
""" # noqa trailing whitespaces are expected
        assert expected == str(d)

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_ceph_analyze.py"
# End:

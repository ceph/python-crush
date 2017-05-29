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
from crush.ceph import Ceph


class TestAnalyze(object):

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
host0     -1     65536      1                    0.0
host1     -2     65536      1                    0.0
host2     -5     65536      1                    0.0

Worst case scenario if a host fails:

        ~over filled %~
~type~                 
device            33.33
host              33.33
rack               0.00
root               0.00\
""" # noqa trailing whitespaces are expected
            assert expected == str(d)

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_ceph_analyze.py"
# End:

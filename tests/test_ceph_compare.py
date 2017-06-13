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


class TestCompare(object):

    def test_sanity_check_args(self):
        a = Ceph().constructor([
            'compare',
        ])
        with pytest.raises(Exception) as e:
            a.pre_sanity_check_args()
        assert 'missing --origin' in str(e.value)

        a = Ceph().constructor([
            'compare',
            '--origin', 'ORIGIN',
            '--destination', 'DESTINATION',
            '--rule', 'RULE',
            '--origin-choose-args', 'ORIGIN CHOOSE ARGS',
            '--destination-choose-args', 'DESTINATION CHOOSE ARGS',
        ])
        a.pre_sanity_check_args()
        a.post_sanity_check_args()

        a = Ceph().constructor([
            'compare',
            '--origin', 'ORIGIN',
            '--destination', 'DESTINATION',
            '--rule', 'RULE',
            '--origin-choose-args', 'ORIGIN CHOOSE ARGS',
            '--destination-choose-args', 'DESTINATION CHOOSE ARGS',
            '--pool', '3',
            '--values-count', '10',
        ])
        a.pre_sanity_check_args()
        with pytest.raises(Exception) as e:
            a.post_sanity_check_args()
        assert 'mutually exclusive' in str(e.value)

# Local Variables:
# compile-command: "cd .. ; tox -e py27 -- -s -vv tests/test_ceph_compare.py"
# End:

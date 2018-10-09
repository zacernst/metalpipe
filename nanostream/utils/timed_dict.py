"""
Copyright (C) 2016 Zachary Ernst
zernst@trunkclub.com or zac.ernst@gmail.com

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import threading
import time
import collections


class TimedDict(dict):
    """
    A dictionary whose keys time out; sends events when keys time out.
    """
    def __init__(self, timeout=10, check_interval=1, timeout_call=None):
        self.timeout = timeout
        self.check_interval = check_interval
        self.base_dict = collections.defaultdict(lambda x: None)
        self.time_dict = {}
        self.timeout_call = timeout_call
        self.last_check_time = time.time()

    def __repr__(self):
        self.clean()
        d = {key: (self.base_dict[key], self.time_dict[key],) for key in self.base_dict.keys()}
        return d.__repr__()

    def __setitem__(self, key, value):
        current_time = time.time()
        self.base_dict[key] = value
        self.time_dict[key] = current_time
        self.clean()

    def clean(self):
        current_time = time.time()
        if current_time - self.last_check_time > self.check_interval:
            key_set = set(self.base_dict.keys())
            for key in key_set:
                if current_time - self.time_dict[key] > self.timeout:
                    del self.time_dict[key]
                    del self.base_dict[key]
        self.last_check_time = current_time

    def __getitem__(self, key):
        out = self.base_dict[key]
        self.clean()
        return out


if __name__ == '__main__':
    d = TimedDict(timeout=10, check_interval=1)
    d['foo'] = 'bar'
    print(d)


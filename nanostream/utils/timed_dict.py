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
import types
import logging
import random


logging.basicConfig(level=logging.DEBUG)


class Empty:
    pass


class TimedDict(collections.MutableMapping):
    """
    A dictionary whose keys time out; sends events when keys time out.
    """
    def __init__(
            self, timeout=10, check_per_second=.5,
            sample_probability=.25, timeout_callback=None,
            expired_keys_ratio=.25):
        self.timeout = timeout
        self.check_per_second = 2
        self.base_dict = {}
        self.expired_keys_ratio = expired_keys_ratio
        self.time_dict = {}
        self.timeout_callback = timeout_callback
        self.sample_probability = sample_probability
        self.clean_thread = threading.Thread(target=self.clean)
        self.clean_thread.start()

    def __len__(self):
        return len(self.base_dict)

    def items(self):
        for i in self.base_dict.items():
            yield i

    def __delitem__(self, key):
        del self.base_dict[key]
        del self.time_dict[key]

    def __iter__(self):
        print('whatever again.')

    def keys(self):
        for i in self.base_dict.keys():
            yield i

    def values(self):
        for i in self.base_dict.values():
            yield i

    def __repr__(self):
        d = {
            key: (self.base_dict[key], self.time_dict[key],)
            for key in self.base_dict.keys()}
        return d.__repr__()

    def __setitem__(self, key, value):
        current_time = time.time()
        self.base_dict[key] = value
        self.time_dict[key] = current_time + self.timeout

    def clean(self):
        while 1:
            current_time = time.time()
            expire_keys = set()
            keys_checked = 0.
            items = list(self.time_dict.items())
            for key, expire_time in items:
                if random.random() > self.sample_probability:
                    continue
                keys_checked += 1
                if current_time >= expire_time:
                    expire_keys.add(key)
                    logging.debug(
                        'marking key for deletion: {key}'.
                        format(key=str(key)))
            for key in expire_keys:
                del self.base_dict[key]
                del self.time_dict[key]
            expired_keys_ratio = (
                len(expire_keys) / keys_checked
                if keys_checked > 0 else 0.)
            if expired_keys_ratio < self.expired_keys_ratio:
                time.sleep(1. / self.check_per_second)

    def __getitem__(self, key):
        if key not in self.base_dict:
            return Empty()
        if time.time() >= self.time_dict[key]:
            logging.debug(
                'deleting expired key: {key}'.
                format(key=str(key)))
            del self.time_dict[key]
            del self.base_dict[key]
        try:
            out = self.base_dict[key]
        except KeyError:
            out = Empty()
        return out


if __name__ == '__main__':
    d = TimedDict(timeout=10)
    d['foo'] = 'bar'
    print(d)
    counter = 0
    while counter < 100:
        d[counter] = random.random()
        time.sleep(random.random() / 100)
        if random.random() < .1 and len(d) > 0:
            try:
                random_key = random.choice(list(d.keys()))
                print(d[random_key])
            except:
                pass
        counter += 1
    print('printing...')
    for i in d.items():
        print(i)

    del d[10]



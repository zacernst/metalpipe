"""
Copyright (C) 2016 Zachary Ernst
zac.ernst@gmail.com

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

from timed_dict import TimedDict
from nanostream_node import NanoNode


class InnerJoin(NanoNode):
    '''
    Joins two streams of dict-like objects.
    '''

    def __init__(self, join_keys=None, expiration_window=5):
        self.join_keys = join_keys

    def start(self):
        pass

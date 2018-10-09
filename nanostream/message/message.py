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

import time
import uuid


class NanoStreamMessage(object):
    """
    A class that contains the message payloads that are queued for
    each ``NanoStreamProcessor``. It holds the messages and lots
    of metadata used for logging, monitoring, etc.
    """

    def __init__(self, message_content):
        self.message_content = message_content
        self.history = []
        self.time_created = time.time()
        self.time_processed = None
        self.uuid = uuid.uuid4()
        self.accumulator = {}

    def __repr__(self):
        s = ': '.join([
            'NanoStreamMessage',
            self.uuid.hex, str(self.message_content)])
        return s

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

import queue
import uuid
import logging
from nanostream_message import NanoStreamMessage
logging.basicConfig(level=logging.DEBUG)

class NanoStreamQueue:
    """
    """
    def __init__(self, max_queue_size, name=None):
        self.queue = queue.Queue(max_queue_size)
        self.name = name or uuid.uuid4().hex
        self.open_for_business = True
        self.source_node = None
        self.target_node = None

    def get(self):
        try:
            message = self.queue.get(block=False)
        except queue.Empty:
            message = None
        return message

    def put(self, message, *args, **kwargs):
        '''
        '''
        previous_message = kwargs['previous_message']
        if not isinstance(message, NanoStreamMessage):
            message_obj = NanoStreamMessage(message)
            message_obj.accumulator[self.source_node.name] = message
            if previous_message is not None:
                message_obj.accumulator.update(previous_message.accumulator)
        logging.debug(
                'Putting message on queue {queue_name}: {message_content}'.format(
                queue_name=self.name, message_content=message_obj.message_content))
        if message_obj.message_content is not None:
            self.queue.put(message_obj)
        logging.debug('Put message on queue: ' + str(message_obj))
        logging.debug('Message history: ' + str(message_obj.accumulator))

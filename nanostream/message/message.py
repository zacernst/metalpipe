"""
NanoStreamMessage module
========================

The ``NanoStreamMesaage`` encapsulates the content of each piece of data,
along with some useful metadata.
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
        self.message_content = (
            message_content if isinstance(message_content, (dict,))
            else {'__value__': message_content})
        self.history = []
        self.time_created = time.time()
        self.time_processed = None
        self.uuid = uuid.uuid4()
        self.accumulator = {}

    def __repr__(self):
        s = ': '.join(
            ['NanoStreamMessage', self.uuid.hex])
        return s

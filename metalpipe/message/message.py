"""
MetalPipeMessage module
========================

The ``MetalPipeMesaage`` encapsulates the content of each piece of data,
along with some useful metadata.
"""

import time
import uuid
import hashlib


class MetalPipeMessage(object):
    """
    A class that contains the message payloads that are queued for
    each ``MetalPipeProcessor``. It holds the messages and lots
    of metadata used for logging, monitoring, etc.
    """

    def __init__(self, message_content):
        if not isinstance(message_content, (dict,)):
            raise Exception(
                "Message content must be a dictionary or "
                "`output_keypath` must be specified."
            )
        self.message_content = message_content
        self.message_hash = hashlib.md5(
            bytes(str(message_content), encoding="utf8")
        )
        self.history = []
        self.time_created = time.time()
        self.time_queued = None
        self.time_processed = None
        self.uuid = uuid.uuid4()
        self.accumulator = {}

    def __repr__(self):
        s = ": ".join(["MetalPipeMessage", self.uuid.hex])
        return s

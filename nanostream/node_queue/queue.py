"""
NanoStreamQueue module
=====================

These are queues that form the directed edges between nodes.
"""

import queue
import uuid
import logging
import time
from nanostream.message.message import NanoStreamMessage
from nanostream.message.batch import BatchStart, BatchEnd
from nanostream import node


QUEUE_TIME_WINDOW = 100


class NanoStreamQueue:
    """
    """

    def __init__(self, max_queue_size, name=None):
        self.queue = queue.Queue(max_queue_size)
        self.name = name or uuid.uuid4().hex
        self.source_node = None
        self.target_node = None
        self.queue_times = []  # Time messages spend in queue


    @property
    def empty(self):
        return self.queue.empty()

    def get(self):
        try:
            message = self.queue.get(block=False)
            self.queue_times.append(time.time() - message.time_queued)
            self.queue_times = self.queue_times[-1 * QUEUE_TIME_WINDOW:]
        except queue.Empty:
            message = None
        logging.debug('Retrieved message: ' + str(message))
        return message

    def put(self, message, *args, previous_message=None, **kwargs):
        '''
        Places a message on the output queues. If the message is ``None``,
        then the queue is skipped.

        Messages are ``NanoStreamMessage`` objects; the payload of the
        message is message.message_content.
        '''
        if isinstance(message, (node.NothingToSeeHere,)):
            return
        elif previous_message is not None:
            previous_message = previous_message.message_content
        else:
            previous_message = {}
        if self.source_node.output_key is not None:
            message = {self.source_node.output_key: message}

        # Check if we need to retain the previous message in the keys of
        # this message, assuming we have dictionaries, etc.
        if self.source_node.retain_input:
            for key, value in previous_message.items():
                if key not in message:
                    message[key] = value
                elif (key in message and value is not None
                        and self.source_node.prefer_existing_value):
                    message[key] = value
                else:
                    pass


        message_obj = NanoStreamMessage(message)
        message_obj.time_queued = time.time()
        self.queue.put(message_obj)

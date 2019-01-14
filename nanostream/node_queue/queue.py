"""
NanoStreamQueue module
=====================

These are queues that form the directed edges between nodes.
"""

import queue
import uuid
import logging
from nanostream.message.message import NanoStreamMessage
from nanostream.message.batch import BatchStart, BatchEnd
from nanostream import node


class NanoStreamQueue:
    """
    """

    def __init__(self, max_queue_size, name=None):
        self.queue = queue.Queue(max_queue_size)
        self.name = name or uuid.uuid4().hex
        self.source_node = None
        self.target_node = None


    @property
    def empty(self):
        return self.queue.empty()

    def get(self):
        try:
            message = self.queue.get(block=False)
        except queue.Empty:
            message = None
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
        if not isinstance(message, (dict,)):
            message = {self.source_node.output_key: message}
        # Check if we need to retain the previous message in the keys of
        # this message, assuming we have dictionaries, etc.
        logging.info(
            '--->' + self.source_node.name + '--->' +
            str(previous_message) + '--->' + str(previous_message.items()
                if previous_message is not None else None))
        for key, value in previous_message.items():
            message[key] = value

        message_obj = NanoStreamMessage(message)
        self.queue.put(message_obj)

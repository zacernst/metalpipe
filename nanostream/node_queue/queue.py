"""
NanoStreamQueue module
======================

These are queues that form the directed edges between nodes.
"""

import queue
import uuid
import logging
from nanostream.message.message import NanoStreamMessage
from nanostream.message.batch import BatchStart, BatchEnd


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
        previous_message = previous_message
        print('--->' + str(previous_message))
        if previous_message is not None:
            previous_message = previous_message.message_content
        # Check if we need to retain the previous message in the keys of
        # this message, assuming we have dictionaries, etc.
        if self.source_node.retain_input:
            logging.info(self.source_node.name)
            keys_values = previous_message.items()
            for key, value in keys_values:
                if key in message:
                    logging.warn(
                        'Key {key} is in the message. Skipping.'.format(
                            key=key))
                    continue
                else:
                    message[key] = value

        if not isinstance(message, (NanoStreamMessage,)):
            message_obj = NanoStreamMessage(message)
        if message_obj.message_content is not None:
            self.queue.put(message_obj)

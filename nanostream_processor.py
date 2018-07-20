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

import queue
import multiprocessing as mp
import threading
import types
import time
import uuid
from nanostream_message import NanoStreamMessage


class NanoStreamSender:
    """
    Anything with an output queue.
    """
    def __init__(self, *args, **kwargs):
        self.output_queue_list = []
        self.message_counter = 0
        self.uuid = uuid.uuid4().hex

    def queue_output(self, message, output_queue_list=None):
        self.message_counter += 1
        for output_queue in self.output_queue_list:
            output_queue.put(message, block=True, timeout=None)

    @property
    def is_source(self):
        return not hasattr(self, 'input_queue_list')


class NanoStreamQueue:
    """
    """
    def __init__(self, max_queue_size, name=None):
        self.queue = queue.Queue(max_queue_size)
        self.name = name or uuid.uuid4().hex

    def get(self):
        try:
            message = self.queue.get(block=False)
        except queue.Empty:
            message = None
        return message

    def put(self, message, *args, **kwargs):
        '''
        '''
        if not isinstance(message, NanoStreamMessage):
            message = NanoStreamMessage(message)
        if message.message_content is not None:
            self.queue.put(message)


class NanoStreamListener:
    """
    Anything that reads from an input queue.
    """

    def __init__(self, workers=1, index=0, child_class=None, **kwargs):
        self.workers = workers
        self.child_class = child_class
        self.message_counter = 0
        self.input_queue_list = []

    def _process_item(self, message):
        """
        This calls the user's ``process_item`` with just the message content,
        and then returns the full message.
        """
        result = self.process_item(message.message_content)
        result = NanoStreamMessage(result)
        return result

    def start(self):
        while 1:
            for input_queue in self.input_queue_list:
                one_item = input_queue.get()
                if one_item is None:
                    continue
                self.message_counter += 1
                output = self._process_item(one_item)
                if hasattr(self, 'queue_output'):
                    self.queue_output(output)


class NanoStreamProcessor(NanoStreamListener, NanoStreamSender):
    """
    """
    def __init__(self, input_queue=None, output_queue=None):
        super(NanoStreamProcessor, self).__init__()
        NanoStreamSender.__init__(self)
        self.start = super(NanoStreamProcessor, self).start

    @property
    def is_sink(self):
        return (
            not hasattr(self, 'output_queue_list') or
            len(self.output_queue_list) == 0)

    @property
    def is_source(self):
        return not hasattr(self, 'input_queue')

    def process_item(self, *args, **kwargs):
        raise Exception("process_item needs to be overridden in child class.")


class DirectoryWatchdog(NanoStreamSender):
    """
    Watches a directory for new or modified files, reads them, sends them
    downstream.
    """
    pass


class PrintStreamProcessor(NanoStreamProcessor):
    """
    Just a class that prints, for testing purposes only.
    """
    def process_item(self, item):
        print(item)
        return item


class ExtractKeysStreamProcessor(NanoStreamProcessor):
    """
    Just extracts the keys from a dictionary. For testing.
    """
    def process_item(self, item):
        output = list(item.keys())
        return output


class CounterOfThings(NanoStreamSender):

    def start(self):
        '''
        Just start counting integers
        '''
        counter = 0
        while 1:
            self.queue_output(counter)
            counter += 1

class DivisibleByThreeFilter(NanoStreamProcessor):
    def process_item(self, message):
        if message % 3 == 0:
            return message

class DivisibleBySevenFilter(NanoStreamProcessor):
    def process_item(self, message):
        if message % 7 == 0:
            return message


class PrinterOfThings(NanoStreamListener):

    def process_item(self, message):
        print(message)

if __name__ == '__main__':
    import nanostream_pipeline

    c = CounterOfThings()
    p = PrinterOfThings()
    divisible_by_three = DivisibleByThreeFilter()
    divisible_by_seven = DivisibleBySevenFilter()
    pipeline = nanostream_pipeline.NanoStreamGraph()
    pipeline.add_node(c)
    pipeline.add_node(divisible_by_three)
    pipeline.add_node(p)
    pipeline.add_node(divisible_by_seven)

    pipeline.add_edge(c, divisible_by_three)
    pipeline.add_edge(c, divisible_by_seven)
    pipeline.add_edge(divisible_by_seven, p)
    pipeline.add_edge(divisible_by_three, p)

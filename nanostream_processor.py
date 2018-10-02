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
import hashlib
import threading
import types
import json
import time
import uuid
import logging
from functools import partialmethod
from nanostream_batch import BatchStart, BatchEnd
from nanostream_message import NanoStreamMessage
# from nanostream_queue import NanoStreamQueue
import nanostream_trigger
# import nanostream_graph
# import bowerbird
import inspect


DEFAULT_MAX_QUEUE_SIZE = 128

logging.basicConfig(level=logging.DEBUG)

def exception(message=None):
    raise Exception(message)


class NanoAncestor:
    '''
    Sometimes a node is just a listener; sometimes just a sender. This helps
    assign attributes to both.
    '''

    def __init__(self, *args, **kwargs):
        self.signature = inspect.signature(self.__class__.__init__)
        self.passthrough = kwargs.get('passthrough', False)
        self.make_global = kwargs.get('make_global', None)
        self.no_process_item = not hasattr(self, 'process_item')
        self.name = kwargs.get('name', None) or uuid.uuid4().hex

    def __gt__(self, other):
        # graph = self.graph
        self.add_edge(other)
        return other

    @property
    def is_source(self):
        return (not hasattr(self, 'input_queue_list') or
            len(self.input_queue_list) == 0)

    @property
    def is_sink(self):
        return (not hasattr(self, 'output_queue_list')
            or len(self.output_queue_list) == 0)

    def partial(self, **kwargs):
        '''
        Return a version of `self` with some parameters filled-in.
        Probably overkill.
        '''
        partial_class = type(
            self.__class__.__name__ + '_partial_' + hashlib.md5(
                bytes(str(kwargs), 'utf8')).hexdigest()[:5],
            (self.__class__,), {'__init__': partialmethod(
                self.__init__, **kwargs)})
        return partial_class

    def _make_global(self, name, value):
        '''
        Puts the value in the `NanoStreamGraph.global_dict` where it can be
        accessed from other nodes. Session information would be an example
        of one possible use.
        '''
        if name in self.graph.global_dict:
            logging.warning(
                'Name {name} already exists in global_dict'.format(name=name))
        self.graph.global_dict[name] = value

    def get_global(self, name, default=None):
        '''
        Just looks up the value of `name` in the `NanoStreamGraph.global_dict`.
        '''
        return self.graph.global_dict.get(name, default)

    def pre_flight_check(self, *args, **kwargs):
        pass  # override for initialization that happens at start

    def add_edge(self, target, **kwargs):
        """
        Create an edge connecting `self` to `target`. The edge
        is really just a queue
        """
        max_queue_size = kwargs.get(
            'max_queue_size', DEFAULT_MAX_QUEUE_SIZE)
        edge_queue = NanoStreamQueue(max_queue_size)

        # Make this recursive below
        if hasattr(target, 'get_source'):  # Only metaclass has this method
            target.get_source().input_queue_list.append(edge_queue)
        else:
            target.input_queue_list.append(edge_queue)

        if hasattr(self, 'get_source'):
            self.get_sink().output_queue_list.append(edge_queue)
        else:
            self.output_queue_list.append(edge_queue)


class NanoStreamSender(NanoAncestor):
    """
    Anything with an output queue.
    """
    def __init__(self, *args, **kwargs):
        self.output_queue_list = []
        self.message_counter = 0
        self.uuid = uuid.uuid4().hex
        super(NanoStreamSender, self).__init__(**kwargs)

    def queue_output(self, message, output_queue_list=None):
        self.message_counter += 1
        logging.debug('Queueing: ' + str(message))
        for output_queue in self.output_queue_list:
            output_queue.put(message, block=True, timeout=None)


class NanoStreamListener(NanoAncestor):
    """
    Anything that reads from an input queue.
    """

    def __init__(self, workers=1, index=0, child_class=None, **kwargs):
        self.workers = workers
        self.child_class = child_class
        self.message_counter = 0
        self.input_queue_list = []
        super(NanoStreamListener, self).__init__(**kwargs)

    def _process_item(self, message):
        """
        This calls the user's ``process_item`` with just the message content,
        and then returns the full message.
        """
        logging.debug('Processing message content: {message_content}'.format(
            message_content=message.mesage_content))
        # Change everything to generators in the processing of
        # messages for nodes that return more than one message output
        # per input.
        for result in self.process_item(messager.message_content):
            result = NanoStreamMessage(result)
            if self.make_global is not None:
                self._make_global(self.make_global, result)
            if self.passthrough:
                logging.debug('Passthrough: ' + str(message))
                result = message
            yield result

    def start(self):
        self.pre_flight_check()
        if self.no_process_item:
            self.queue_output(nanostream_trigger.Trigger())
            while 1:
                time.sleep(1)
        else:
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
    def __init__(self, input_queue=None, output_queue=None, **kwargs):
        super(NanoStreamProcessor, self).__init__(**kwargs)
        NanoStreamSender.__init__(self, **kwargs)
        self.start = super(NanoStreamProcessor, self).start

    @property
    def is_sink(self):
        return (
            not hasattr(self, 'output_queue_list') or
            len(self.output_queue_list) == 0)


if __name__ == '__main__':
    import nanostream_pipeline

    c = CounterOfThings()
    p = PrinterOfThings()
    e = ConstantEmitter(thing='foobar', delay=2)
    divisible_by_three = DivisibleByThreeFilter()
    divisible_by_seven = DivisibleBySevenFilter()
    pipeline = nanostream_pipeline.NanoStreamGraph()
    pipeline.add_node(e)
    pipeline.add_node(p)
    pipeline.add_edge(e, p)

    '''
    pipeline.add_node(c)
    pipeline.add_node(divisible_by_three)
    pipeline.add_node(p)
    pipeline.add_node(divisible_by_seven)
    pipeline.add_edge(c, divisible_by_three)
    pipeline.add_edge(c, divisible_by_seven)
    pipeline.add_edge(divisible_by_seven, p)
    pipeline.add_edge(divisible_by_three, p)
    '''

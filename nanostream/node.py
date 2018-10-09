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
import logging
import os
import threading
import functools
import csv
import io
import yaml
import types
import inspect

from nanostream.message.batch import BatchStart, BatchEnd
from nanostream.message.message import NanoStreamMessage
from nanostream.queue.queue import NanoStreamQueue
from nanostream.message.canary import Canary
from nanostream.message.poison_pill import PoisonPill


DEFAULT_MAX_QUEUE_SIZE = os.environ.get('DEFAULT_MAX_QUEUE_SIZE', 128)


logging.basicConfig(level=logging.DEBUG)


class NanoNode:
    '''
    The foundational class of `NanoStream`. This class is inherited by all
    nodes in a computation graph.
    '''

    def __init__(self, *args, **kwargs):
        self.name = kwargs.get('name', None) or uuid.uuid4().hex
        self.input_queue_list = []
        self.output_queue_list = []
        self.input_node_list = []
        self.output_node_list = []
        self.global_dict = None
        self.thread_dict = {}
        self.kill_thread = False
        self.accumulator = {}

    def __gt__(self, other):
        self.add_edge(other)
        return other

    def run_generator(self):
        return any(queue.open_for_business for queue in self.output_queue_list)

    @property
    def is_source(self):
        return len(self.input_queue_list) == 0

    @property
    def is_sink(self):
        return len(self.output_queue_list) == 0

    def add_edge(self, target, **kwargs):
        """
        Create an edge connecting `self` to `target`. The edge
        is really just a queue
        """
        max_queue_size = kwargs.get(
            'max_queue_size', DEFAULT_MAX_QUEUE_SIZE)
        edge_queue = NanoStreamQueue(max_queue_size)

        self.output_node_list.append(target)
        # set output_node_list to be same object as sink output_node_list
        target.input_node_list.append(self)

        edge_queue.source_node = self
        edge_queue.target_node = target

        # Make this recursive below
        # TODO: Add support for composition
        if 0 and hasattr(target, 'get_source'):  # Only metaclass has this method
            target.get_source().input_queue_list.append(edge_queue)
        else:
            target.input_queue_list.append(edge_queue)

        if 0 and hasattr(self, 'get_source'):
            self.get_sink().output_queue_list.append(edge_queue)
        else:
            self.output_queue_list.append(edge_queue)

    def start(self):
        if self.is_source and not isinstance(self, (DynamicClassMediator,)):
            for output in self.generator():
                yield output, None
                if not any(queue.open_for_business for queue in self.output_queue_list):
                    logging.info('shutting down.')
                    break
        else:
            while 1:
                for input_queue in self.input_queue_list:
                    one_item = input_queue.get()
                    if one_item is None:
                        continue
                    message_content = one_item.message_content
                    if isinstance(message_content, (PoisonPill,)):
                        logging.info('received poision pill.')
                        self.kill_thread = True
                    elif message_content is None:
                        pass
                    else:
                        for output in self.process_item(message_content):
                            yield output, one_item  # Also yield previous message
                if self.kill_thread:
                    break
            for input_queue in self.input_queue_list:
                input_queue.open_for_business = False


    def process_item(self, message):
        '''
        Default `process_item` method for broadcast queues. With this method
        guaranteed to exist, we can handle poison pills and canaries in the
        `_process_item` method.
        '''
        pass

    def processor(self, message):
        """
        This calls the user's ``process_item`` with just the message content,
        and then returns the full message.
        """
        logging.debug('Processing message content: {message_content}'.format(
            message_content=message.mesage_content))
        for result in self.process_item(message.message_content):
            result = NanoStreamMessage(result)
            yield result

    def stream(self):
        for output, previous_message in self.start():
            for output_queue in self.output_queue_list:
                output_queue.put(
                    output,
                    block=True,
                    timeout=None,
                    previous_message=previous_message)

    def all_connected(self, seen=None):
        seen = seen or set()

        if isinstance(self, (DynamicClassMediator,)):
            for node_name, node_dict in self.node_dict.items():
                node_obj = node_dict['obj']
                seen = seen | node_obj.all_connected(seen=seen)
        else:
            if self not in seen:
                seen.add(self)
            for node in self.input_node_list + self.output_node_list:
                if node in seen:
                    continue
                seen.add(node)
                seen = seen | node.all_connected(seen=seen)
        return seen

    def broadcast(self, broadcast_message):
        for node in self.all_connected():
            for input_queue in node.input_queue_list:
                input_queue.put(broadcast_message)

    def global_start(self):
        thread_dict = self.thread_dict
        for node in self.all_connected():
            logging.debug('global_start:' + str(self))
            thread = threading.Thread(target=NanoNode.stream, args=(node,))
            thread.start()
            node.thread_dict = thread_dict
            thread_dict[node.name] = thread

    def terminate(self):
        self.broadcast(PoisonPill())


class CounterOfThings(NanoNode):

    def generator(self):
        '''
        Just start counting integers
        '''
        counter = 0
        while 1:
            yield counter
            counter += 1


class PrinterOfThings(NanoNode):

    def __init__(self, prepend='printer:'):
        self.prepend = prepend
        super(PrinterOfThings, self).__init__()

    def process_item(self, message):
        print(self.prepend + str(message))
        yield message


class ConstantEmitter(NanoNode):
    '''
    Send a thing every n seconds
    '''
    def __init__(
            self, thing=None, thing_key=None, delay=2, from_json=False):
        if from_json:
            thing = json.loads(thing)

        self.thing = json.loads(thing) if from_json else thing
        self.thing_key = thing_key
        self.delay = delay or 0
        super(ConstantEmitter, self).__init__()
        logging.debug(
            'init constant emitter with constant {thing}'.format(
                thing=str(thing)))

    def generator(self):
        logging.debug('starting constant emitter')
        while 1:
            time.sleep(self.delay)
            output = (
                {self.thing_key: self.thing} if self.thing_key is not None
                else self.thing)
            logging.debug('output:' + str(output))
            yield output
            logging.debug('yielded output: {output}'.format(output=output))


class LocalFileReader(NanoNode):

    def __init__(
        self, directory='.', send_batch_markers=True,
            serialize=False, read_mode='r'):
        self.directory = directory
        self.serialize = serialize
        self.read_mode = read_mode
        self.send_batch_markers = send_batch_markers
        super(LocalFileReader, self).__init__()

    def process_item(self, message):
        filename = '/'.join([self.directory, message])
        with open(filename, self.read_mode) as file_obj:
            if self.serialize:
                if self.send_batch_markers:
                    yield BatchStart()
                for line in file_obj:
                    output = line
                    yield output
                yield BatchEnd()
            else:
                output = file_obj.read()
                yield output


class CSVReader(NanoNode):

    def __init__(self, send_batch_markers=True):
        self.send_batch_markers = send_batch_markers
        super(CSVReader, self).__init__()

    def process_item(self, message):
        file_obj = io.StringIO(message)
        reader = csv.DictReader(file_obj)
        if self.send_batch_markers:
            yield BatchStart()
        for row in reader:
            yield row
        if self.send_batch_markers:
            yield BatchEnd()


class LocalDirectoryWatchdog(NanoNode):

    def __init__(self, directory='.', check_interval=3):
        self.directory = directory
        self.latest_arrival = time.time()
        self.check_interval = check_interval
        super(LocalDirectoryWatchdog, self).__init__()

    def generator(self):
        while self.run_generator():
            logging.debug('sleeping...')
            time.sleep(self.check_interval)
            time_in_interval = None
            for filename in os.listdir(self.directory):
                last_modified_time = os.path.getmtime(
                    '/'.join([self.directory, filename]))
                if last_modified_time > self.latest_arrival:
                    yield '/'.join([self.directory, filename])
                    if (
                        time_in_interval is None or
                            last_modified_time > time_in_interval):
                        time_in_interval = last_modified_time
                        logging.debug(
                            'time_in_interval: ' + str(time_in_interval))
            logging.debug('done looping over filenames')
            if time_in_interval is not None:
                self.latest_arrival = time_in_interval


class HttpGetRequest(NanoNode):
    '''
    Makes GET requests.
    '''
    def __init__(self, url=None, endpoint=None, endpoint_dict=None):

        self.url = url
        self.endpoint = endpoint
        self.endpoint_dict = endpoint_dict or {}
        super(HttpGetRequest, self).__init__()

    def process_item(self, message):
        '''
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a dictionary
        with keys and values to be passed in the GET request.
        '''

        # Hit the parameterized endpoint and yield back the results
        self.current_endpoint_dict = endpoint_dict
        get_response = self.pipeline.session.get(
            self.url.format(**endpoint_dict),
            cookies=self.pipeline.cookies)
        self.key_value.update(endpoint_dict)
        return get_response.text


class DynamicClassMediator(NanoNode):

    def __init__(self, *args, **kwargs):

        super(DynamicClassMediator, self).__init__(**kwargs)

        for node_name, node_dict in self.node_dict.items():
            cls_obj = node_dict['cls_obj']
            node_obj = cls_obj(**kwargs)
            node_dict['obj'] = node_obj

        for edge in self.raw_config['edges']:
            source_node_obj = self.node_dict[edge['from']]['obj']
            target_node_obj = self.node_dict[edge['to']]['obj']
            source_node_obj > target_node_obj

        def bind_methods():
            for attr_name in dir(DynamicClassMediator):
                if attr_name.startswith('_'):
                    continue
                attr_obj = getattr(DynamicClassMediator, attr_name)
                if not isinstance(attr_obj, types.FunctionType):
                    continue
                setattr(self, attr_name, types.MethodType(attr_obj, self))

        bind_methods()

        source = self.get_source()
        self.input_queue_list = source.input_queue_list
        sink = self.get_sink()
        self.output_queue_list = sink.output_queue_list

        self.output_node_list = sink.output_node_list
        self.input_node_list = source.input_node_list

    def get_sink(self):
        sinks = self.sink_list()
        if len(sinks) > 1:
            raise Exception(
                '`DynamicClassMediator` may have no more than one sink.')
        elif len(sinks) == 0:
            return None
        return sinks[0]

    def get_source(self):
        sources = self.source_list()
        if len(sources) > 1:
            raise Exception(
                '`DynamicClassMediator` may have no more than one source.')
        elif len(sources) == 0:
            return None
        return sources[0]

    def sink_list(self):
        sink_nodes = []
        for node_name, node_dict in self.node_dict.items():
            node_obj = node_dict['obj']
            if len(node_obj.output_queue_list) == 0:
                sink_nodes.append(node_obj)
        return sink_nodes

    def source_list(self):
        source_nodes = [
            node_dict['obj'] for node_dict in self.node_dict.values()
            if node_dict['obj'].is_source]
        return source_nodes

    def hi(self):
        return 'hi'


def get_node_dict(node_config):
    node_dict = {}
    for node_config in node_config['nodes']:
        node_class = globals()[node_config['class']]
        node_name = node_config['name']
        node_dict[node_name] = {}
        node_dict[node_name]['class'] = node_class
        frozen_arguments = node_config.get('frozen_arguments', {})
        node_dict[node_name]['frozen_arguments'] = frozen_arguments
        node_obj = node_class(**frozen_arguments)
        # node_dict[node_name]['obj'] = node_obj
        node_dict[node_name]['remapping'] = node_config.get('arg_mapping', {})
    return node_dict


def kwarg_remapper(f, **kwarg_mapping):
    reverse_mapping = {value: key for key, value in kwarg_mapping.items()}
    logging.debug('kwarg_mapping:' + str(kwarg_mapping))
    parameters = [i for i, _ in list(inspect.signature(f).parameters.items())]
    for kwarg in parameters:
        if kwarg not in kwarg_mapping:
            reverse_mapping[kwarg] = kwarg

    # kwarg_mapping: {thing: outer_thing}
    # reverse_mapping: {thing_key: thing_key, outer_thing: thing}

    def remapped_function(*args, **kwargs):
        remapped_kwargs = {}
        for key, value in kwargs.items():
            if key in reverse_mapping:
                remapped_kwargs[reverse_mapping[key]] = value
        logging.debug('remaed function with kwargs: ' + str(remapped_kwargs))

        #remapped_kwargs = {
        #    reverse_mapping[argument]: value for argument, value
        #    in kwargs.items() if argument in parameters}
        return f(*args, **remapped_kwargs)

    return remapped_function


def template_class(
    class_name, parent_class, kwargs_remapping,
        frozen_arguments_mapping):

    kwargs_remapping = kwargs_remapping or {}
    frozen_init = functools.partial(
        parent_class.__init__, **frozen_arguments_mapping)
    if isinstance(parent_class, (str,)):
        parent_class = globals()[parent_class]
    cls = type(class_name, (parent_class,), {})
    setattr(
        cls, '__init__', kwarg_remapper(
            frozen_init, **kwargs_remapping))
    return cls


def class_factory(raw_config):
    new_class = type(raw_config['name'], (DynamicClassMediator,), {})
    new_class.node_dict = get_node_dict(raw_config)
    new_class.class_name = raw_config['name']
    new_class.edge_list_dict = raw_config.get('edges', [])
    new_class.raw_config = raw_config

    for node_name, node_config in new_class.node_dict.items():
        _class = node_config['class']
        cls = template_class(
            node_name, _class,
            node_config['remapping'],
            node_config['frozen_arguments'])
        setattr(cls, 'raw_config', raw_config)
        node_config['cls_obj'] = cls
    # Inject?
    globals()[new_class.__name__] = new_class
    return new_class


if __name__ == '__main__':

    raw_config = yaml.load(
        open('./__nanostream_modules__/csv_watcher.yaml', 'r'))
    class_factory(raw_config)
    obj = CSVWatcher(watch_directory='./sample_data')

    printer = PrinterOfThings(prepend='TWO: ')
    obj > printer
    obj.node_dict['printer']['obj'].name = 'printer'
    obj.node_dict['csv_reader']['obj'].name = 'csv_reader'
    obj.global_start()

    get_post = HttpGetRequest(
        url='https://jsonplaceholder.typicode.com',
        endpoint='posts/{post_number}',
        endpoint_dict={})

    url_output = PrinterOfThings()


    # def __init__(self, url=None, endpoint=None, endpoint_dict=None):

    #c = CounterOfThings()
    #p = PrinterOfThings()
    #c = CounterOfThings()
    #e = ConstantEmitter(thing='foobar', delay=2)
    #e > p > c

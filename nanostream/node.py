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
import sys
import functools
import csv
import io
import yaml
import types
import inspect
import MySQLdb

from timed_dict.timed_dict import TimedDict
from nanostream.message.batch import BatchStart, BatchEnd
from nanostream.message.message import NanoStreamMessage
from nanostream.node_queue.queue import NanoStreamQueue
from nanostream.message.canary import Canary
from nanostream.message.poison_pill import PoisonPill
from nanostream.utils.set_attributes import set_kwarg_attributes
from nanostream.utils.data_structures import Row, MySQLTypeSystem
from nanostream.utils import data_structures as ds

DEFAULT_MAX_QUEUE_SIZE = os.environ.get('DEFAULT_MAX_QUEUE_SIZE', 128)

logging.basicConfig(level=logging.DEBUG)


def no_op(*args, **kwargs):
    '''
    No-op function to serve as default ``pre_flight_function``.
    '''
    return None


def get_environment_variables(environment_variables=None):
    environment_variables = environment_variables or []
    environment = {
        environment_variable: os.environ.get(environment_variable, None)
        for environment_variable in environment_variables}
    return environment


class NanoNode:
    '''
    The foundational class of `NanoStream`. This class is inherited by all
    nodes in a computation graph.

    Order of operations:
    1. Child class ``__init__`` function
    2. ``NanoNode`` ``__init__`` function
    3. ``preflight_function`` (Specified in initialization params)
    4. ``setup``
    5. start

    '''

    def __init__(
        self,
        *args,
        batch=False,
        pre_flight_function=no_op,
        pre_flight_function_args=None,
        pre_flight_function_kwargs=None,
        pre_flight_destinations=None,
        globalize_output=None,
        globalize_aggregate_output=None,
        globalize_pre_flight_function=False,
            **kwargs):
        self.name = kwargs.get('name', None) or uuid.uuid4().hex
        self.input_queue_list = []
        self.output_queue_list = []
        self.input_node_list = []
        self.output_node_list = []
        self.global_dict = None  # We'll add a dictionary upon startup
        self.thread_dict = {}
        self.kill_thread = False
        self.accumulator = {}
        self.batch = batch
        self.globalize_output = globalize_output
        self.globalize_aggregate_output = globalize_aggregate_output
        self.pre_flight_function = pre_flight_function
        self.pre_flight_function_args = pre_flight_function_args or tuple()
        self.pre_flight_function_kwargs = pre_flight_function_kwargs or {}
        self.pre_flight_destinations = pre_flight_destinations or {}
        self.globalize_pre_flight_function = (
            globalize_pre_flight_function)  # Not sure about this

    def setup(self):
        '''
        To be overridden by child classes when we need to do something
        after setting attributes and the pre-flight function.
        '''
        pass


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
        max_queue_size = kwargs.get('max_queue_size', DEFAULT_MAX_QUEUE_SIZE)
        edge_queue = NanoStreamQueue(max_queue_size)

        self.output_node_list.append(target)
        # set output_node_list to be same object as sink output_node_list
        target.input_node_list.append(self)

        edge_queue.source_node = self
        edge_queue.target_node = target

        # Make this recursive below
        # TODO: Add support for composition
        target.input_queue_list.append(edge_queue)
        self.output_queue_list.append(edge_queue)

    def start(self):
        # Run pre-flight function and save output if necessary
        logging.info('Starting node: {node}'.format(
            node=self.__class__.__name__))
        if self.pre_flight_function is not None:
            pre_flight_results = self.pre_flight_function(
                *self.pre_flight_function_args,
                **self.pre_flight_function_kwargs) or {}
            if self.pre_flight_destinations is not None:
                if not isinstance(self.pre_flight_destinations, (dict,)):
                    raise Exception(
                        'Trying pre-flight function, but '
                        'targets are not given in a dictionary.')
                for key, value in pre_flight_results.items():
                    setattr(self, self.pre_flight_destinations[key], value)

        # We have to separate the pre-flight function, the setup of the
        # class, and any necessary startup functions (such as connecting
        # to a database).

        self.setup()  # Setup function?

        if self.is_source and not isinstance(self, (DynamicClassMediator, )):
            for output in self.generator():
                if self.globalize_output is not None:
                    self.global_dict[self.globalize_output] = output
                if self.globalize_aggregate_output is not None:
                    self.global_dict[self.globalize_aggregate_output].\
                    append(output)
                yield output, None
                if not any(queue.open_for_business
                           for queue in self.output_queue_list):
                    logging.info('shutting down.')
                    break
        else:
            logging.debug('About to enter loop for reading input queue in {node}.'.format(node=str(self)))
            while 1:
                for input_queue in self.input_queue_list:
                    one_item = input_queue.get()
                    if one_item is None:
                        continue
                    logging.debug('Got item: {one_item}'.format(one_item=str(one_item)))
                    message_content = one_item.message_content
                    if isinstance(message_content, (PoisonPill, )):
                        logging.info('received poision pill.')
                        self.kill_thread = True
                    elif message_content is None:
                        pass
                    else:
                        for output in self.process_item(message_content):
                            if self.globalize_output is not None:
                                self.global_dict[
                                    self.globalize_output] = output
                            if self.globalize_aggregate_output is not None:
                                self.global_dict[
                                    self.globalize_aggregate_output].\
                                append(output)
                            yield output, one_item  # yield previous message
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

        if isinstance(self, (DynamicClassMediator, )):
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
        global_dict = {}
        for node in self.all_connected():
            node.global_dict = global_dict  # Establishing shared globals
            logging.debug('global_start:' + str(self))
            thread = threading.Thread(target=NanoNode.stream, args=(node, ))
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


class StreamMySQLTable(NanoNode):
    def __init__(
        self,
        *args,
        host='localhost',
        user=None,
        table=None,
        password=None,
        database=None,
        port=3306,
        to_row_obj=False,
        batch=True,
            **kwargs):
        self.host = host
        self.user = user
        self.to_row_obj = to_row_obj
        self.password = password
        self.database = database
        self.port = port
        self.batch = batch
        self.table = table

        super(StreamMySQLTable, self).__init__(**kwargs)

    def setup(self):
        self.db = MySQLdb.connect(
            passwd=self.password,
            db=self.database,
            user=self.user,
            port=self.port)
        self.cursor = MySQLdb.cursors.DictCursor(self.db)

        self.table_schema_query = ('''SELECT column_name, column_type '''
                                   '''FROM information_schema.columns '''
                                   '''WHERE table_name='{table}';'''.format(
                                       table=self.table))

        self.table_schema = self.get_schema()
        # Need a mapping from header to MYSQL TYPE
        for mapping in self.table_schema:
            column = mapping['column_name']
            type_string = mapping['column_type']
            this_type = ds.MySQLTypeSystem.type_mapping(type_string)
            # Start here:
            #    store the type_mapping
            #    use it to cast the data into the MySQLTypeSchema
            #    ensure that the generator is emitting MySQLTypeSchema objects

    def get_schema(self):
        self.cursor.execute(self.table_schema_query)
        table_schema = self.cursor.fetchall()
        return table_schema

    def generator(self):
        if self.batch:
            yield BatchStart(schema=self.table_schema)
        self.cursor.execute(
            """SELECT * FROM {table};""".format(table=self.table))
        result = self.cursor.fetchone()
        while result is not None:
            if self.to_row_obj:
                result = Row.from_dict(result, type_system=MySQLTypeSystem)
            yield result
            result = self.cursor.fetchone()
        if self.batch:
            yield BatchEnd()


class PrinterOfThings(NanoNode):

    @set_kwarg_attributes()
    def __init__(self, prepend='printer:'):
        super(PrinterOfThings, self).__init__()
        logging.info('Initialized printer...')

    def process_item(self, message):
        print(self.prepend + str(message))
        yield message


class ConstantEmitter(NanoNode):
    '''
    Send a thing every n seconds
    '''

    @set_kwarg_attributes()
    def __init__(self, thing=None, thing_key=None, delay=2):

        super(ConstantEmitter, self).__init__()
        logging.debug('init constant emitter with constant {thing}'.format(
            thing=str(thing)))

    def generator(self):
        logging.debug('starting constant emitter')
        while 1:
            time.sleep(self.delay)
            output = ({
                self.thing_key: self.thing
            } if self.thing_key is not None else self.thing)
            logging.debug('output:' + str(output))
            yield output
            logging.debug('yielded output: {output}'.format(output=output))


class TimeWindowAccumulator(NanoNode):
    '''
    Every N seconds, put the latest M seconds data on the queue.
    '''

    @set_kwarg_attributes()
    def __init__(self, time_window=None, send_interval=None):
        pass


class LocalFileReader(NanoNode):
    @set_kwarg_attributes()
    def __init__(self,
                 directory='.',
                 send_batch_markers=True,
                 serialize=False,
                 read_mode='r'):
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
    @set_kwarg_attributes()
    def __init__(self, send_batch_markers=True, to_row_obj=True):
        super(CSVReader, self).__init__()

    def process_item(self, message):
        file_obj = io.StringIO(message)
        reader = csv.DictReader(file_obj)
        if self.send_batch_markers:
            yield BatchStart()
        for row in reader:
            if self.to_row_obj:
                row = Row.from_dict(row)
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
                last_modified_time = os.path.getmtime('/'.join(
                    [self.directory, filename]))
                if last_modified_time > self.latest_arrival:
                    yield '/'.join([self.directory, filename])
                    if (time_in_interval is None
                            or last_modified_time > time_in_interval):
                        time_in_interval = last_modified_time
                        logging.debug('time_in_interval: ' +
                                      str(time_in_interval))
            logging.debug('done looping over filenames')
            if time_in_interval is not None:
                self.latest_arrival = time_in_interval


class HttpGetRequest(NanoNode):
    '''
    Makes GET requests.
    '''

    @set_kwarg_attributes()
    def __init__(
            self, url=None, endpoint=None, endpoint_dict=None, json=True):
        self.endpoint.update(endpoint_dict)
        super(HttpGetRequest, self).__init__()

    def process_item(self, item):
        '''
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a dictionary
        with keys and values to be passed in the GET request.
        '''

        # Hit the parameterized endpoint and yield back the results
        get_response = self.pipeline.session.get(
            self.endpoint.format(**(item or {})),
            cookies=self.pipeline.cookies)
        return get_response.json() if self.json else get_response.text


class SimpleJoin(NanoNode):
    '''
    Joins two streams on a key, using exact match only. MVP.
    '''

    def __init__(self, key, window=30):
        self.key = key
        self.window = window
        self.timed_dict = TimedDict(timeout=window)
        super(SimpleJoin, self).__init__()

    def process_item(self, message):
        '''
        Assumes that `message` is a `Row` object.
        '''

        if isinstance(message, (
                BatchStart,
                BatchEnd,
        )):
            pass
        else:
            key = getattr(message, self.key).value
            if isinstance(self.timed_dict[key], Row):
                self.timed_dict[key] = self.timed_dict[key].concat(
                    message, fail_on_duplicate=False)
                yield self.timed_dict[key]
            else:
                self.timed_dict[key] = message


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

    def remapped_function(*args, **kwargs):
        remapped_kwargs = {}
        for key, value in kwargs.items():
            if key in reverse_mapping:
                remapped_kwargs[reverse_mapping[key]] = value
        logging.debug('renamed function with kwargs: ' + str(remapped_kwargs))

        return f(*args, **remapped_kwargs)

    return remapped_function


def template_class(class_name, parent_class, kwargs_remapping,
                   frozen_arguments_mapping):

    kwargs_remapping = kwargs_remapping or {}
    frozen_init = functools.partial(parent_class.__init__,
                                    **frozen_arguments_mapping)
    if isinstance(parent_class, (str, )):
        parent_class = globals()[parent_class]
    cls = type(class_name, (parent_class, ), {})
    setattr(cls, '__init__', kwarg_remapper(frozen_init, **kwargs_remapping))
    return cls


def class_factory(raw_config):
    new_class = type(raw_config['name'], (DynamicClassMediator, ), {})
    new_class.node_dict = get_node_dict(raw_config)
    new_class.class_name = raw_config['name']
    new_class.edge_list_dict = raw_config.get('edges', [])
    new_class.raw_config = raw_config

    for node_name, node_config in new_class.node_dict.items():
        _class = node_config['class']
        cls = template_class(node_name, _class, node_config['remapping'],
                             node_config['frozen_arguments'])
        setattr(cls, 'raw_config', raw_config)
        node_config['cls_obj'] = cls
    # Inject?
    globals()[new_class.__name__] = new_class
    return new_class


if __name__ == '__main__':
    globals().update(ds.make_types())  # Insert types into namespace
    employees_table = StreamMySQLTable(
        password='imadfs1',
        database='employees',
        table='employees',
        pre_flight_function=get_environment_variables,
        pre_flight_function_args=(('MYSQL_USER',),),
        pre_flight_destinations={'MYSQL_USER': 'user'})
    printer = PrinterOfThings()

    employees_table > printer
    employees_table.global_start()


    #raw_config = yaml.load(
    #    open('./__nanostream_modules__/csv_watcher.yaml', 'r'))
    #class_factory(raw_config)
    #csv_watcher = CSVWatcher(watch_directory='./sample_data')

    #joiner = SimpleJoin(key='first_name')

    #employees_table > joiner
    #csv_watcher > joiner
    #joiner > printer

    #printer.global_start()

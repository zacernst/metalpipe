"""
Node module
===========

This is the node module.
"""

import time
import datetime
import uuid
import logging
import os
import threading
import sys
import copy
import functools
import csv
import io
import yaml
import types
import inspect
import MySQLdb
import requests

from timed_dict.timed_dict import TimedDict
from nanostream.message.batch import BatchStart, BatchEnd
from nanostream.message.message import NanoStreamMessage
from nanostream.node_queue.queue import NanoStreamQueue
from nanostream.message.canary import Canary
from nanostream.message.poison_pill import PoisonPill
from nanostream.utils.set_attributes import set_kwarg_attributes
from nanostream.utils.data_structures import Row, MySQLTypeSystem
from nanostream.utils import data_structures as ds
from nanostream.utils.paginated_get_requests import PaginatedHttpGetRequest, SafeMap

DEFAULT_MAX_QUEUE_SIZE = os.environ.get('DEFAULT_MAX_QUEUE_SIZE', 128)

logging.basicConfig(level=logging.DEBUG)



def no_op(*args, **kwargs):
    '''
    No-op function to serve as default ``get_runtime_attrs``.
    '''
    return None


def get_environment_variables(*args):
    '''
    Retrieves the environment variables listed in ``*args``.

    Args:
        args (list of str): List of environment variables.
    Returns:
        dict: Dictionary of environment variables to values. If the environment
            variable is not defined, the value is ``None``.
    '''
    environment_variables = args or []
    environment = {
        environment_variable: os.environ.get(environment_variable, None)
        for environment_variable in environment_variables}
    return environment


class Parameters:

    def __init__(self, **kwargs):
        self.parameters = kwargs


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


    These methods have the following intended uses:

    1. ``__init__`` Sets attribute values and calls the ``NanoNode`` ``__init__``
       method.
    2. ``get_runtime_attrs`` Sets any attribute values that are to be determined
       at runtime, e.g. by checking environment variables or reading values
       from a database. The ``get_runtime_attrs`` should return a dictionary
       of attributes -> values, or else ``None``.
    3. ``setup`` Sets the state of the ``NanoNode`` and/or creates any attributes
       that require information available only at runtime.

    :ivar send_batch_markers: If ``True``, then a ``BatchStart`` marker will
        be sent when a new input is received, and a ``BatchEnd`` will be sent
        after the input has been processed. The intention is that a number of
        items will be emitted for each input received. For example, we might
        emit a table row-by-row for each input.
    :ivar get_runtime_attrs: A function that returns a dictionary-like object.
        The keys and values will be saved to this ``NanoNode`` object's
        attributes. The function is executed one time, upon starting the node.
    :ivar get_runtime_attrs_args: A tuple of arguments to be passed to the
        ``get_runtime_attrs`` function upon starting the node.
    :ivar get_runtime_attrs_kwargs: A dictionary of kwargs passed to the
        ``get_runtime_attrs`` function.
    :ivar runtime_attrs_destinations: If set, this is a dictionary mapping
        the keys returned from the ``get_runtime_attrs`` function to the
        names of the attributes to which the values will be saved.
    :ivar throttle: For each input received, a delay of ``throttle`` seconds
        will be added.
    :ivar keep_alive: If ``True``, keep the node's thread alive after
        everything has been processed.
    :ivar name: The name of the node. Defaults to a randomly generated hash.
        Note that this hash is not consistent from one run to the next.
    :ivar input_mapping: When the node receives a dictionary-like object,
        this dictionary will cause the keys of the dictionary to be remapped
        to new keys.
    :ivar retrain_input: If ``True``, then combine the dictionary-like input
        with the output. If keys clash, the output value will be kept.

    '''

    def __init__(
        self,
        *args,
        batch=False,
        get_runtime_attrs=no_op,
        get_runtime_attrs_args=None,
        get_runtime_attrs_kwargs=None,
        runtime_attrs_destinations=None,
        input_mapping=None,
        retain_input=False,
        throttle=0,
        keep_alive=True,
        name=None,
            **kwargs):
        self.name = name or uuid.uuid4().hex
        self.input_mapping = input_mapping or {}
        self.input_queue_list = []
        self.output_queue_list = []
        self.input_node_list = []
        self.output_node_list = []
        self.global_dict = None  # We'll add a dictionary upon startup
        self.thread_dict = {}
        self.kill_thread = False
        self.accumulator = {}
        self.keep_alive = keep_alive
        self.retain_input = retain_input  # Keep the input dictionary and send it downstream
        self.throttle = throttle
        self.get_runtime_attrs = get_runtime_attrs
        self.get_runtime_attrs_args = get_runtime_attrs_args or tuple()
        self.get_runtime_attrs_kwargs = get_runtime_attrs_kwargs or {}
        self.runtime_attrs_destinations = runtime_attrs_destinations or {}

    def setup(self):
        '''
        To be overridden by child classes when we need to do something
        after setting attributes and the pre-flight function.
        '''
        logging.debug('No ``setup`` method for {class_name}.'.format(
            class_name=self.__class__.__name__))
        pass

    def __gt__(self, other):
        '''
        Convenience method so that we can link two nodes by ``node1 > node2``.
        '''
        self.add_edge(other)
        return other

    def run_generator(self):
        return any(queue.open_for_business for queue in self.output_queue_list)

    @property
    def is_source(self):
        '''
        Tests whether the node is a source or not, i.e. whether there are no
        inputs to the node.
        '''
        return len(self.input_queue_list) == 0

    @property
    def is_sink(self):
        '''
        Tests whether the node is a sink or not, i.e. whether there are no
        outputs from the node.
        '''
        return len(self.output_queue_list) == 0

    def add_edge(self, target, **kwargs):
        """
        Create an edge connecting `self` to `target`. The edge
        is really just a queue
        """
        max_queue_size = kwargs.get('max_queue_size', DEFAULT_MAX_QUEUE_SIZE)
        edge_queue = NanoStreamQueue(max_queue_size)

        self.output_node_list.append(target)
        target.input_node_list.append(self)

        edge_queue.source_node = self
        edge_queue.target_node = target

        # Make this recursive below
        # TODO: Add support for composition
        target.input_queue_list.append(edge_queue)
        self.output_queue_list.append(edge_queue)

    def start(self):
        '''
        Starts the node. This is called by ``NanoNode.global_start()``.
        '''

        # Run pre-flight function and save output if necessary
        logging.debug('Starting node: {node}'.format(
            node=self.__class__.__name__))
        # ``get_runtime_attrs`` returns a dict-like object whose keys and
        # values are stored as attributes of the ``NanoNode`` object.
        if self.get_runtime_attrs is not None:
            pre_flight_results = self.get_runtime_attrs(
                *self.get_runtime_attrs_args,
                **self.get_runtime_attrs_kwargs) or {}
            if self.runtime_attrs_destinations is not None:
                for key, value in pre_flight_results.items():
                    setattr(self, self.runtime_attrs_destinations[key], value)
            elif self.runtime_attrs_destinations is None:
                for key, value in pre_flight_results.items():
                    setattr(self, key, value)
            else:
                raise Exception(
                    'There is a ``get_runtime_attrs``, but the '
                    '``runtime_attrs_destinations`` is neither None nor a '
                    'dict-like object.')

        # We have to separate the pre-flight function, the setup of the
        # class, and any necessary startup functions (such as connecting
        # to a database).

        self.setup()  # Setup function?

        if self.is_source and not isinstance(self, (DynamicClassMediator, )):
            for output in self.generator():
                yield output, None
                if not any(queue.open_for_business
                           for queue in self.output_queue_list):
                    logging.debug('shutting down.')
                    break
            while self.keep_alive:
                time.sleep(1)
        else:
            logging.debug(
                'About to enter loop for reading input queue in {node}.'.format(
                    node=str(self)))
            while 1:
                for input_queue in self.input_queue_list:
                    one_item = input_queue.get()
                    if one_item is None:
                        continue

                    time.sleep(self.throttle)
                    logging.debug('Got item: {one_item}'.format(
                        one_item=str(one_item)))
                    message_content = one_item.message_content
                    if (
                        isinstance(message_content, (dict,))
                        and len(message_content) == 1
                            and '__value__' in message_content):
                        message_content = message_content['__value__']
                    # If we receive a ``PoisonPill`` object, kill the thread.
                    if isinstance(message_content, (PoisonPill,)):
                        logging.debug('received poision pill.')
                        self.kill_thread = True
                    # If we receive ``None``, then pass.
                    elif message_content is None:
                        pass
                    # Otherwise, process the message as usual, by calling
                    # the ``NanoNode`` object's ``process_item`` method.
                    else:
                        self.message = message_content

                        # Remap inputs; if input_mapping is a string,
                        # assume we're mapping to {input_mapping: value}, else
                        # assume we're renaming keys

                        if isinstance(self.input_mapping, (str,)):
                            self.message = {self.input_mapping: self.message}
                        elif (
                            isinstance(self.input_mapping, (dict,)) and
                            len(self.input_mapping) > 0 and
                                isinstance(self.message, (dict,))):
                            for key, value in list(self.message.items()):
                                if key in self.input_mapping:
                                    self.message[self.input_mapping[key]] = value
                        elif (
                            isinstance(self.input_mapping, (dict,)) and
                            len(self.input_mapping) > 0 and
                            hasattr(self.message, '__dict__') and
                                not isinstance(self.message, (dict,))):
                            for key, value in self.message.__dict__.items():
                                if key in self.input_mapping:
                                    self.message.__dict__[self.input_mapping[key]] = value
                        elif (
                            isinstance(self.input_mapping, (dict,)) and
                                len(self.input_mapping) > 0):
                            raise Exception('Bad case in input remapping.')
                        else:
                            pass

                        for output in self.process_item():
                            logging.info('one_item: ' + str(one_item))
                            yield output, one_item  # yield previous message
                if self.kill_thread:
                    break
            for input_queue in self.input_queue_list:
                input_queue.open_for_business = False


    def process_item(self, *args, **kwargs):
        '''
        Default `process_item` method for broadcast queues. With this method
        guaranteed to exist, we can handle poison pills and canaries in the
        `_process_item` method.
        '''
        pass

    def processor(self):
        """
        This calls the user's ``process_item`` with just the message content,
        and then returns the full message.
        """
        logging.debug('Processing message content: {message_content}'.format(
            message_content=message.mesage_content))
        for result in self.process_item():
            result = NanoStreamMessage(result)
            yield result

    def stream(self):
        '''
        Called in each ``NanoNode`` thread. This is a generator that yields
        the output from the node.
        '''
        for output, previous_message in self.start():
            logging.debug('In NanoNode.stream.stream() --> ' + str(output))
            for output_queue in self.output_queue_list:
                output_queue.put(
                    output,
                    block=True,
                    timeout=None,
                    previous_message=previous_message)

    def all_connected(self, seen=None):
        '''
        Returns all the nodes connected (directly or indirectly) to ``self``.

        Args:
            seen (set): A set of all the nodes that have been identified as
                connected to ``self``.
        Returns:
            (set of ``NanoNode``): All the nodes connected to ``self``. This
                includes ``self``.
        '''
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
        '''
        Puts the message into all the input queues for all connected nodes.
        '''
        for node in self.all_connected():
            for input_queue in node.input_queue_list:
                input_queue.put(broadcast_message)

    def global_start(self):
        '''
        Starts every node connected to ``self``.
        '''
        # thread_dict = self.thread_dict
        global_dict = {}
        for node in self.all_connected():
            node.global_dict = global_dict  # Establishing shared globals
            logging.debug('global_start:' + str(self))
            thread = threading.Thread(target=NanoNode.stream, args=(node, ))
            thread.start()
            node.thread_dict = self.thread_dict
            self.thread_dict[node.name] = thread
        monitor_thread = threading.Thread(
            target=NanoNode.thread_monitor, args=(self, ))
        monitor_thread.start()

    def thread_monitor(self):
        while 1:
            time.sleep(1)
            for node_name, thread in self.thread_dict.items():
                if not thread.isAlive():
                    self.terminate()
                    raise Exception(
                        'Thread from node {node} dead. '
                        'All nodes terminated.'.format(node=node_name))

    def terminate(self):
        self.broadcast(PoisonPill())


class CounterOfThings(NanoNode):
    def foo__init__(self, *args, start=0, end=None, **kwargs):
        self.start = start
        self.end = end
        super(CounterOfThings, self).__init__(*args, **kwargs)

    def generator(self):
        '''
        Just start counting integers
        '''
        counter = 1
        while 1:
            yield counter
            counter += 1
            if counter > 10:
                assert False


class SubstituteRegex(NanoNode):
    def __init__(self, match_regex, substitute_string, *args, **kwargs):
        self.match_regex = match_regex
        self.substitute_string = substitute_string
        self.regex_obj = re.compile(self.match_regex)
        super(SubstituteRegex, self).__init__(*args, **kwargs)

    def process_item(self):
        pass


class SequenceEmitter(NanoNode):
    '''
    Emits ``sequence`` ``max_sequences`` times, or forever if
    ``max_sequences`` is ``None``.
    '''

    def __init__(self, sequence, *args, max_sequences=None, **kwargs):
        self.sequence = sequence
        self.max_sequences = max_sequences
        super(SequenceEmitter, self).__init__(*args, **kwargs)

    def generator(self):
        '''
        Emit the sequence ``max_sequences`` times.
        '''
        counter = 0
        while self.max_sequences is None or counter < self.max_sequences:
            for item in self.sequence:
                yield item
            counter += 1


class GetEnvironmentVariables(NanoNode):
    def __init__(self, *args, mappings=None, **kwargs):
        self.environment_mappings = mappings or {}
        self.environment_variables = args or []
        super(GetEnvironmentVariables, self).__init__(**kwargs)

    def generator(self):
        environment = {
            self.environment_mappings.get(
                environment_variable, environment_variable): os.environ.get(
                    environment_variable, None)
                for environment_variable in self.environment_variables}
        yield environment

    def process_item(self):
        environment = {
            self.environment_mappings.get(
                environment_variable, environment_variable): os.environ.get(
                    environment_variable, None)
                for environment_variable in self.environment_variables}
        yield environment


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
        send_batch_markers=True,
            **kwargs):
        self.host = host
        self.user = user
        self.to_row_obj = to_row_obj
        self.password = password
        self.database = database
        self.port = port
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
        if self.send_batch_markers:
            yield BatchStart(schema=self.table_schema)
        self.cursor.execute(
            """SELECT * FROM {table};""".format(table=self.table))
        result = self.cursor.fetchone()
        while result is not None:
            if self.to_row_obj:
                result = Row.from_dict(result, type_system=MySQLTypeSystem)
            yield result
            result = self.cursor.fetchone()
        if self.send_batch_markers:
            yield BatchEnd()



class PrinterOfThings(NanoNode):

    @set_kwarg_attributes()
    def __init__(self, prepend='printer: '):
        super(PrinterOfThings, self).__init__()
        logging.debug('Initialized printer...')

    def process_item(self):
        print(self.prepend + str(self.message))
        yield self.message


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

    def process_item(self):
        filename = '/'.join([self.directory, self.message])
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

    def process_item(self):
        file_obj = io.StringIO(self.message)
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

    def __init__(
        self,
        url=None,
        endpoint='',
        endpoint_dict=None,
        json=True,
            **kwargs):
        self.endpoint = endpoint
        self.url = url
        self.endpoint_dict = endpoint_dict or {}
        self.json = json
        self.endpoint_dict.update(self.endpoint_dict)

        super(HttpGetRequest, self).__init__(**kwargs)

    def process_item(self):
        '''
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a dictionary
        with keys and values to be passed in the GET request.

        Three use-cases:
        1. Endpoint and parameters set initially and never changed.
        2. Endpoint and parameters set once at runtime
        3. Endpoint and parameters set by upstream messages
        '''

        # Hit the parameterized endpoint and yield back the results
        logging.debug('HttpGetRequest --->' + str(self.message))
        get_response = requests.get(
            self.endpoint.format(**(self.message or {})))
        output = get_response.json() if self.json else get_response.text
        yield output


class SimpleJoin(NanoNode):
    '''
    Joins two streams on a key, using exact match only. MVP.
    '''

    def __init__(self, key, window=30):
        self.key = key
        self.window = window
        self.timed_dict = TimedDict(timeout=window)
        super(SimpleJoin, self).__init__()

    def process_item(self):
        '''
        Assumes that `message` is a `Row` object.
        '''

        if isinstance(self.message, (
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

class HttpGetRequestPaginator(NanoNode):

    def __init__(
        self,
        endpoint_dict=None,
        json=True,
        pagination_get_request_key=None,
        endpoint_template=None,
        additional_data_key=None,
        pagination_key=None,
        default_offset_value='',
            **kwargs):
        self.pagination_get_request_key = pagination_get_request_key
        self.additional_data_key = additional_data_key
        self.pagination_key = pagination_key
        self.endpoint_dict = endpoint_dict or {}
        self.endpoint_template = endpoint_template
        self.default_offset_value = default_offset_value

        self.endpoint_template = self.endpoint_template.format_map(SafeMap(**endpoint_dict))

        # import pdb; pdb.set_trace()
        super(HttpGetRequestPaginator, self).__init__(**kwargs)

    def process_item(self):
        self.requestor = PaginatedHttpGetRequest(
            pagination_get_request_key=self.pagination_get_request_key,
            endpoint_template=self.endpoint_template.format_map(SafeMap(**(self.message or {}))),
            additional_data_key=self.additional_data_key,
            pagination_key=self.pagination_key,
            default_offset_value=self.default_offset_value)

        for i in self.requestor.responses():
            yield i


def get_value(
        dictionary, path, delimiter='.', default_value=None):
    dictionary = copy.deepcopy(dictionary)
    if isinstance(path, (str,)):
        path = path.split(delimiter)
    elif isinstance(path, (list, tuple,)):
            pass
    else:
        raise Exception('what?')
    for step in path:
        dictionary = dictionary.get(step, default_value)
    return dictionary


def set_value(dictionary, path, value):
    for step in path[:-1]:
        dictionary = dictionary[step]
    dictionary[path[-1]] = value


def iterate_leaves(dictionary, keypath=None):
    keypath = keypath or []
    for key, value in dictionary.items():
        if not isinstance(value, (dict,)):
            yield keypath + [key], value
        else:
            for i in iterate_leaves(value, keypath=keypath + [key]):
                yield i

def remap_dictionary(source_dictionary, target_dictionary):
    for path, value in iterate_leaves(target_dictionary):
        set_value(target_dictionary, path, get_value(source_dictionary, value))

d = {'foo': 'bar', 'baz': {'qux': 'foobar'}}
e = {'hi': ['foo'], 'whatever': ['baz', 'qux']}

remap_dictionary(d, e)

if __name__ == '__main__':
    ONE_DAY = 3600 * 24 * 1000
    NOW = int(float(time.time())) * 1000
    TWO_WEEKS_AGO = str(
        int(float(NOW - (ONE_DAY / 4))))

    HUBSPOT_TEMPLATE = (
        'https://api.hubapi.com/email/public/v1/'
        'events?hapikey={HUBSPOT_API_KEY}&'
        'startTimestamp={start_timestamp}&'
        'endTimestamp={end_timestamp}&'
        'limit=5000&'
        'offset={offset}')

    endpoint_dict = {
        # 'hubspot_api_key': HUBSPOT_API_KEY,
        'start_timestamp': TWO_WEEKS_AGO,
        'end_timestamp': NOW}

    paginator = HttpGetRequestPaginator(
        endpoint_dict=endpoint_dict,
        pagination_get_request_key='offset',
        endpoint_template=HUBSPOT_TEMPLATE,
        additional_data_key='hasMore',
        pagination_key='offset',
        name='Hubspot_paginator')

    env_vars = GetEnvironmentVariables(
        'HUBSPOT_API_KEY',
        'HUBSPOT_USER_ID',
        name='environment_variables')
    printer = PrinterOfThings()
    env_vars > paginator # > printer
    env_vars.global_start()


def foo():
    env_vars = GetEnvironmentVariables('MYSQL_USER', 'PYTHONPATH')
    employees_table = StreamMySQLTable(
        password='imadfs1',
        database='employees',
        table='employees',
        get_runtime_attrs=get_environment_variables,
        get_runtime_attrs_args=('MYSQL_USER',),
        runtime_attrs_destinations={'MYSQL_USER': 'user'})
    printer = PrinterOfThings()

    employees_table > printer
    # employees_table.global_start()

    post_counter = CounterOfThings()
    printer = PrinterOfThings()
    printer_2 = PrinterOfThings()
    printer_3 = PrinterOfThings()
    post_endpoint = 'https://jsonplaceholder.typicode.com/posts/{post_number}'
    post_service = HttpGetRequest(
        endpoint=post_endpoint, throttle=1, input_mapping='post_number')

    user_endpoint = 'https://jsonplaceholder.typicode.com/users/{user_number}'
    user_service = HttpGetRequest(
        endpoint=user_endpoint,
        throttle=1,
        input_mapping={'userId': 'user_number'},
        name='user_service')


    # Need a way to remap inputs and outputs
    post_counter > printer
    post_counter > post_service > printer_2 > user_service > printer_3
    post_counter.global_start()




    #raw_config = yaml.load(
    #    open('./__nanostream_modules__/csv_watcher.yaml', 'r'))
    #class_factory(raw_config)
    #csv_watcher = CSVWatcher(watch_directory='./sample_data')

    #joiner = SimpleJoin(key='first_name')

    #employees_table > joiner
    #csv_watcher > joiner
    #joiner > printer

    #printer.global_start()

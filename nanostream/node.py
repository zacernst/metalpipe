"""
Node module
===========

The ``node`` module contains the ``NanoNode`` class, which is the foundation
for NanoStream.
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
import prettytable
import MySQLdb
import requests
import graphviz

from timed_dict.timed_dict import TimedDict
from nanostream.message.batch import BatchStart, BatchEnd
from nanostream.message.message import NanoStreamMessage
from nanostream.node_queue.queue import NanoStreamQueue
from nanostream.message.canary import Canary
from nanostream.message.poison_pill import PoisonPill
from nanostream.utils.set_attributes import set_kwarg_attributes
from nanostream.utils.data_structures import Row, MySQLTypeSystem
from nanostream.utils import data_structures as ds
from nanostream.utils.helpers import remap_dictionary, get_value

from datadog import api, statsd, ThreadStats
from datadog import initialize as initialize_datadog

DEFAULT_MAX_QUEUE_SIZE = int(os.environ.get('DEFAULT_MAX_QUEUE_SIZE', 128))
MONITOR_INTERVAL = 1
STATS_COUNTER_MODULO = 4

logging.basicConfig(level=logging.ERROR)



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

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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
    :ivar retain_input: If ``True``, then combine the dictionary-like input
        with the output. If keys clash, the output value will be kept.
    :ivar input_message_keypath: Read the value in this keypath as the content
        of the incoming message.

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
        input_message_keypath=None,
        messages_received_counter=0,
        messages_sent_counter=0,
            **kwargs):
        self.name = name or uuid.uuid4().hex
        self.input_mapping = input_mapping or {}
        self.input_queue_list = []
        self.output_queue_list = []
        self.input_node_list = []
        self.input_message_keypath = input_message_keypath or []
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
        self.messages_received_counter = messages_received_counter
        self.messages_sent_counter = messages_sent_counter
        self.instantiated_at = datetime.datetime.now()
        self.started_at = None
        self.stopped_at = None
        self.finished = False
        self.terminate = False
        self.status = 'stopped'  # running, error, success

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
        self.started_at = datetime.datetime.now()
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
                if self.terminate:
                    break
                yield output, None
            self.finished = True
        else:
            logging.debug(
                'About to enter loop for reading input queue in {node}.'.format(
                    node=str(self)))
            while not self.finished:
                new_input_sentinal = False  # Set to ``True`` when we have a new input
                for input_queue in self.input_queue_list:
                    if self.terminate:
                        self.finished
                        continue
                    one_item = input_queue.get()
                    if one_item is None:
                        continue

                    # Keep track of where the message came from, useful for
                    # managing streaming joins, e.g.
                    message_source = input_queue.source_node

                    new_input_sentinal = True
                    self.messages_received_counter += 1

                    time.sleep(self.throttle)
                    logging.debug('Got item: {one_item}'.format(
                        one_item=str(one_item)))
                    # Get the content of a specific keypath, if one has
                    # been defined in the ``NanoNode`` initialization.
                    message_content = (
                        get_value(
                            one_item.message_content,
                            self.input_message_keypath)
                        if len(self.input_message_keypath) > 0
                        else one_item.message_content)
                    if (
                        isinstance(message_content, (dict,))
                        and len(message_content) == 1
                            and '__value__' in message_content):
                        message_content = message_content['__value__']
                    # If we receive a ``PoisonPill`` object, kill the thread.
                    if isinstance(message_content, (PoisonPill,)):
                        logging.debug('received poision pill.')
                        self.finished = True
                        break
                    # If we receive ``None``, then pass.
                    elif message_content is None:
                        pass
                    # Otherwise, process the message as usual, by calling
                    # the ``NanoNode`` object's ``process_item`` method.
                    else:
                        self.message = message_content
                        self.message_source = message_source
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

                        # Datadog
                        if self.datadog:
                            self.datadog_stats.increment(
                                '{pipeline_name}.{node_name}.input_counter'.format(
                                    pipeline_name=self.pipeline_name,
                                    node_name=self.name))

                        for output in self.process_item():
                            if self.datadog:
                                self.datadog_stats.increment(
                                    '{pipeline_name}.{node_name}.output_counter'.format(
                                        pipeline_name=self.pipeline_name,
                                        node_name=self.name))
                            # logging.info('one_item: ' + str(one_item))
                            yield output, one_item  # yield previous message
                if self.kill_thread:
                    break
                # Check input node(s) here to see if they're all ``.finished``
                this_node_finished = (
                    all(
                        node.finished for node in self.input_node_list) and
                    all(queue.empty for queue in self.input_queue_list))
                if this_node_finished:
                    self.finished = True

    def terminate_pipeline(self):
        for node in self.all_connected():
            node.terminate = True

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
        Called in each ``NanoNode`` thread.
        '''
        self.status = 'running'
        try:
            for output, previous_message in self.start():
                logging.debug('In NanoNode.stream.stream() --> ' + str(output))
                for output_queue in self.output_queue_list:
                    self.messages_sent_counter += 1
                    output_queue.put(
                        output,
                        block=True,
                        timeout=None,
                        previous_message=previous_message)
        except Exception as error:
            self.status = 'error'
            self.stopped_at = datetime.datetime.now()
            raise error
        self.status = 'success'
        self.stopped_at = datetime.datetime.now()

    @property
    def time_running(self):
        if self.status == 'stopped':
            return None
        elif self.status == 'running':
            return datetime.datetime.now() - self.started_at
        else:
            return self.stopped_at - self.started_at

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

    def global_start(self, datadog=False, pipeline_name=None):
        '''
        Starts every node connected to ``self``.
        '''
        # thread_dict = self.thread_dict
        global_dict = {}

        # Initialize datadog if we're doing that
        datadog_stats = ThreadStats() if datadog else None
        if datadog:
            datadog_options = {
                'api_key': os.environ['DATADOG_API_KEY'],
                'app_key': os.environ['DATADOG_APP_KEY']}
            initialize_datadog(**datadog_options)
            datadog_stats.start()

        for node in self.all_connected():
            # Set the pipeline name on the attribute of each node
            node.pipeline_name = pipeline_name or uuid.uuid4().hex
            node.datadog_stats = datadog_stats
            # Tell each node whether they're logging to datadog
            node.datadog = datadog
            node.global_dict = global_dict  # Establishing shared globals
            logging.debug('global_start:' + str(self))
            thread = threading.Thread(target=NanoNode.stream, args=(node, ))
            thread.start()
            node.thread_dict = self.thread_dict
            self.thread_dict[node.name] = thread
        monitor_thread = threading.Thread(
            target=NanoNode.thread_monitor, args=(self, ))
        monitor_thread.start()

    @property
    def input_queue_size(self):
        return sum(
            [input_queue.queue.qsize() for input_queue in self.input_queue_list])

    def draw_pipeline(self):
        dot = graphviz.Digraph()
        for node in self.all_connected():
            dot.node(node.name, node.name, shape='box')
        for node in self.all_connected():
            for target_node in node.output_node_list:
                dot.edge(node.name, target_node.name)
        dot.render('test-output/round-table.gv', view=True)

    def thread_monitor(self):
        counter = 0
        pipeline_finished = all(
            node.finished for node in self.all_connected())
        while not pipeline_finished:
            time.sleep(MONITOR_INTERVAL)
            # Check no threads have unexpectedly crashed
            #for node_name, thread in self.thread_dict.items():
            #    if not thread.isAlive():
            #        self.terminate()
            #        raise Exception(
            #            'Thread from node {node} dead. '
            #            'Terminating all nodes.'.format(node=node_name))
            counter += 1

            # Check whether all the workers have ``.finished``
            pipeline_finished = all(
                node.finished for node in self.all_connected())
            if pipeline_finished:
                logging.info('Exiting successfully.')
                for thread in threading.enumerate():
                    if thread.name == 'MainThread':
                        continue
                    elif thread is threading.current_thread():
                        continue
                    else:
                        print(thread)

                sys.exit(0)

            if counter % STATS_COUNTER_MODULO == 0:
                table = prettytable.PrettyTable(
                    ['Node', 'Class', 'Received', 'Sent',
                     'Queued', 'Status', 'Time'])
                for node in self.all_connected():
                    if node.status == 'running':
                        status_color = bcolors.WARNING
                    elif node.status == 'stopped':
                        status_color = ''
                    elif node.status == 'error':
                        status_color = bcolors.FAIL
                    elif node.status == 'success':
                        status_color = bcolors.OKGREEN
                    else:
                        assert False

                    table.add_row(
                        [
                            node.name,
                            node.__class__.__name__,
                            node.messages_received_counter,
                            node.messages_sent_counter,
                            node.input_queue_size,
                            status_color + node.status + bcolors.ENDC,
                            node.time_running])
                print(table)


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

class RandomSample(NanoNode):
    def __init__(self, sample=.1):
        self.sample = sample

    def process_item(self):
        yield self.message if random.random() <= self.sample else None

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
    def __init__(self, mappings=None, environment_variables=None, **kwargs):
        self.environment_mappings = mappings or {}
        self.environment_variables = environment_variables or []
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


class Serializer(NanoNode):
    '''
    Takes an iterable thing as input, and successively yields its items.
    '''

    def __init__(self, *args, **kwargs):
        super(Serializer, self).__init__(**kwargs)

    def process_item(self):
        print(self.message)
        for item in self.message:
            yield item


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
    def __init__(self, prepend='printer: ', **kwargs):
        super(PrinterOfThings, self).__init__(**kwargs)
        logging.debug('Initialized printer...')

    def process_item(self):
        print(self.prepend + str(self.message))
        print('\n')
        yield self.message


class ConstantEmitter(NanoNode):
    '''
    Send a thing every n seconds
    '''

    @set_kwarg_attributes()
    def __init__(self, thing=None, thing_key=None, delay=2, **kwargs):

        super(ConstantEmitter, self).__init__(**kwargs)
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
    def __init__(self, time_window=None, send_interval=None, **kwargs):
        pass


class LocalFileReader(NanoNode):
    @set_kwarg_attributes()
    def __init__(self,
                 directory='.',
                 send_batch_markers=True,
                 serialize=False,
                 read_mode='r',
                 **kwargs):
        super(LocalFileReader, self).__init__(**kwargs)

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
    def __init__(self, send_batch_markers=True, to_row_obj=True, **kwargs):
        super(CSVReader, self).__init__(**kwargs)

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
    def __init__(self, directory='.', check_interval=3, **kwargs):
        self.directory = directory
        self.latest_arrival = time.time()
        self.check_interval = check_interval
        super(LocalDirectoryWatchdog, self).__init__(**kwargs)

    def generator(self):
        while self.keep_alive:
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


class StreamingJoin(NanoNode):
    '''
    Joins two streams on a key, using exact match only. MVP.
    '''

    def __init__(self, window=30, streams=None, *args, **kwargs):
        self.window = window
        self.streams = streams
        self.stream_paths = streams
        self.buffers = {
            stream_name: TimedDict(timeout=self.window)
            for stream_name in self.stream_paths.keys()}
        super(StreamingJoin, self).__init__(*args, **kwargs)

    def process_item(self):
        '''
        '''
        # import pdb; pdb.set_trace()
        value_to_match = get_value(self.message, self.stream_paths[self.message_source.name])
        # Check for matches in all other streams.
        # If complete set of matches, yield the merged result
        # If not, add it to the `TimedDict`.
        print(value_to_match)
        yield('hi')


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


class Remapper(NanoNode):

    def __init__(self, mapping=None, **kwargs):
        self.remapping_dict = mapping or {}
        super(Remapper, self).__init__(**kwargs)

    def process_item(self):
        out = remap_dictionary(self.message, self.remapping_dict)
        yield out


if __name__ == '__main__':
    pass

"""
Node module
===========

The ``node`` module contains the ``NanoNode`` class, which is the foundation
for NanoStream.
"""

import time
import datetime
import uuid
import importlib
import logging
import os
import threading
import pprint
import sys
import copy
import random
import functools
import csv
import re
import io
import yaml
import types
import inspect
import prettytable
# import MySQLdb  #  Fix this for container script
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
from nanostream.utils.helpers import (
    replace_by_path, remap_dictionary, set_value, get_value, to_bool,
    aggregate_values)

from datadog import api, statsd, ThreadStats
from datadog import initialize as initialize_datadog

DEFAULT_MAX_QUEUE_SIZE = int(os.environ.get('DEFAULT_MAX_QUEUE_SIZE', 128))
MONITOR_INTERVAL = 1
STATS_COUNTER_MODULO = 4
LOGJAM_THRESHOLD = .25

PROMETHEUS = False


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
    '''
    This class holds the values for the various colors that are used in the
    tables that monitor the status of the nodes.
    '''

    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class NothingToSeeHere:
    '''
    Vacuous class used as a no-op message type.
    '''
    pass


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
        retain_input=True,
        throttle=0,
        keep_alive=True,
        max_errors=0,
        name=None,
        input_message_keypath=None,
        key=None,
        messages_received_counter=0,
        prefer_existing_value=False,
        messages_sent_counter=0,
        post_process_function=None,
        post_process_keypath=None,
        summary='',
        post_process_function_kwargs=None,
        output_key=None,
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
        self.prefer_existing_value = prefer_existing_value
        self.accumulator = {}
        self.output_key = output_key
        self.keep_alive = keep_alive
        self.retain_input = retain_input  # Keep the input dictionary and send it downstream
        self.throttle = throttle
        self.get_runtime_attrs = get_runtime_attrs
        self.get_runtime_attrs_args = get_runtime_attrs_args or tuple()
        self.get_runtime_attrs_kwargs = get_runtime_attrs_kwargs or {}
        self.runtime_attrs_destinations = runtime_attrs_destinations or {}
        self.key = key
        self.messages_received_counter = messages_received_counter
        self.messages_sent_counter = messages_sent_counter
        self.instantiated_at = datetime.datetime.now()
        self.started_at = None
        self.stopped_at = None
        self.finished = False
        self.error_counter = 0
        self.status = 'stopped'  # running, error, success
        self.max_errors = max_errors
        self.post_process_function_name = post_process_function  # Function to be run on result
        self.post_process_function_kwargs = post_process_function_kwargs or {}
        self.summary = summary
        self.prometheus_objects = None
        self.logjam_score = {'polled': 0., 'logjam': 0.}

        # Get post process function if one is named
        if self.post_process_function_name is not None:
            components = self.post_process_function_name.split('__')
            if len(components) == 1:
                module = None
                function_name = components[0]
                self.post_process_function = globals()[function_name]
            else:
                module = '.'.join(components[:-1])
                function_name = components[-1]
                module = importlib.import_module(module)
                self.post_process_function = getattr(module, function_name)
        else:
            self.post_process_function = None

        self.post_process_keypath = (
            post_process_keypath.split('.') if post_process_keypath is not None
            else None)

    def setup(self):
        '''
        For classes that require initialization at runtime, which can't be done
        when the class's ``__init__`` function is called. The ``NanoNode`` base
        class's setup function is just a logging call.

        It should be unusual to have to make use of ``setup`` because in practice,
        initialization can be done in the ``__init__`` function.
        '''
        logging.debug('No ``setup`` method for {class_name}.'.format(
            class_name=self.__class__.__name__))
        pass

    def __gt__(self, other):
        '''
        Convenience method so that we can link two nodes by ``node1 > node2``.
        This just calls ``add_edge``.
        '''
        self.add_edge(other)
        return other

    @property
    def is_source(self):
        '''
        Tests whether the node is a source or not, i.e. whether there are no
        inputs to the node.

        Returns:
           (bool): ``True`` if the node has no inputs, ``False`` otherwise.

        '''
        return len(self.input_queue_list) == 0

    @property
    def is_sink(self):
        '''
        Tests whether the node is a sink or not, i.e. whether there are no
        outputs from the node.

        Returns:
           (bool): ``True`` if the node has no output nodes, ``False`` otherwise.
        '''
        return len(self.output_queue_list) == 0

    def add_edge(self, target, **kwargs):
        """
        Create an edge connecting `self` to `target`.

        This method instantiates the ``NanoStreamQueue`` object that connects the
        nodes. Connecting the nodes together consists in (1) adding the queue to
        the other's ``input_queue_list`` or ``output_queue_list`` and (2) setting
        the queue's ``source_node`` and ``target_node`` attributes.

        Args:
           target (``NanoNode``): The node to which ``self`` will be connected.
        Returns:
           None
        """
        max_queue_size = kwargs.get('max_queue_size', DEFAULT_MAX_QUEUE_SIZE)
        edge_queue = NanoStreamQueue(max_queue_size)

        self.output_node_list.append(target)
        target.input_node_list.append(self)

        edge_queue.source_node = self
        edge_queue.target_node = target

        target.input_queue_list.append(edge_queue)
        self.output_queue_list.append(edge_queue)

    def start(self):
        '''
        Starts the node. This is called by ``NanoNode.global_start()``.

        The node's main loop is contained in this method. The main loop does
        the following:

        1. records the timestamp to the node's ``started_at`` attribute.
        #. calls ``get_runtime_attrs`` (TODO: check if we can deprecate this)
        #. calls the ``setup`` method for the class (which is a no-op by default)
        #. if the node is a source, then successively yield all the results of
           the node's ``generator`` method, then exit.
        #. if the node is not a source, then loop over the input queues, getting
           the next message. Note that when the message is pulled from the queue,
           the ``NanoStreamQueue`` yields it as a dictionary.
        #. gets either the content of the entire message if the node has no ``key``
           attribute, or the value of ``message[self.key]``.
        #. remaps the message content if a ``remapping`` dictionary has been
           given in the node's configuration
        #. calls the node's ``process_item`` method, yielding back the results.
           (Note that a single input message may cause the node to yield zero,
           one, or more than one output message.)
        #. places the results into each of the node's output queues.
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
                yield output, None
            self.finished = True
        else:
            logging.debug(
                'About to enter loop for reading input queue in {node}.'.format(
                    node=str(self)))
            while not self.finished:
                new_input_sentinal = False  # Set to ``True`` when we have a new input
                for input_queue in self.input_queue_list:
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

                        for output in self._process_item():
                            if self.datadog:
                                self.datadog_stats.increment(
                                    '{pipeline_name}.{node_name}.output_counter'.format(
                                        pipeline_name=self.pipeline_name,
                                        node_name=self.name))
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
            self.cleanup()

    def cleanup(self):
        '''
        If there is any cleanup (closing files, shutting down database connections),
        necessary when the node is stopped, then the node's class should provide
        a ``cleanup`` method. By default, the method is just a logging statement.
        '''
        self.log_info('Cleanup called after shutdown.')

    def log_info(self, message=''):
        logging.debug('{node_name}: {message}'.format(
            node_name=self.name, message=message))

    def terminate_pipeline(self, error=False):
        '''
        This method can be called on any node in a pipeline, and it will cause
        all of the nodes to terminate if they haven't stopped already.

        Args:
           error (bool): Not yet implemented.
        '''
        for node in self.all_connected():
            if not node.finished:
                node.stopped_at = datetime.datetime.now()
                node.finished = True

    def process_item(self, *args, **kwargs):
        '''
        Default no-op for nodes.
        '''
        pass

    @property
    def __message__(self):
        '''
        If the node has an ``output_key`` defined, return the corresponding
        value in the message dictionary. If it does not, return the entire
        message dictionary.

        Nodes should access the content of their incoming message via this
        property.
        '''
        if self.key is None:
            out = self.message
        elif isinstance(self.key, (str,)):
            out = self.message[self.key]
        elif isinstance(self.key, (list,)):
            out = get_value(self.message, self.key)
        else:
            raise Exception('Bad type for input key.')
        return out

    def _process_item(self, *args, **kwargs):
        '''
        This method wraps the node's ``process_item`` method. It provides a place
        to insert code for logging, error handling, etc.

        There's lots of experimental code here, particularly the code for
        Prometheus monitoring.
        '''
        # Swap out the message if ``key`` is specified
        # If we're using prometheus, then increment a counter
        if self.prometheus_objects is not None:
            self.prometheus_objects['incoming_message_summary'].observe(
                random.random())
        message_arrival_time = time.time()

        try:
            for out in self.process_item(*args, **kwargs):
                if (not isinstance(out, (dict, NothingToSeeHere))
                        and self.output_key is None):
                    logging.debug(
                        'Exception raised due to no key' + str(self.name))
                    raise Exception(
                        'Either message must be a dictionary or `output_key` '
                        'must be specified. {name}'.format(self.name))
                # Apply post_process_function if it's defined
                if self.post_process_function is not None:
                    set_value(
                        out,
                        self.post_process_keypath,
                        self.post_process_function(
                            get_value(
                                out,
                                self.post_process_keypath),
                            **self.post_process_function_kwargs))
                if self.prometheus_objects is not None:
                    self.prometheus_objects[
                        'outgoing_message_summary'].set(
                            time.time() - message_arrival_time)

                yield out
        except Exception as err:
            self.error_counter += 1
            if self.error_counter > self.max_errors:
                logging.warning(
                    'message: ' + str(err.args) +
                    str(self.__class__.__name__) + str(self.name))
                raise err
            else:
                logging.warning('oops')

    def processor_bak(self):
        """
        This calls the user's ``process_item`` with just the message content,
        and then returns the full message.

        I think this is deprecated, which is why it's been renamed to ``processor_bak``.
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
        '''
        Return the number of wall-clock seconds elapsed since the node was
        started.
        '''

        if self.status == 'stopped':
            return None
        elif self.status == 'running':
            return datetime.datetime.now() - self.started_at
        elif self.stopped_at is None:
            return datetime.datetime.now() - self.started_at
        else:
            return self.stopped_at - self.started_at

    def all_connected(self, seen=None):
        '''
        Returns all the nodes connected (directly or indirectly) to ``self``.
        This allows us to loop over all the nodes in a pipeline even if we
        have a handle on only one. This is used by ``global_start``, for
        example.

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

    @property
    def logjam(self):
        '''
        Returns the logjam score, which measures the degree to which the
        node is holding up progress in downstream nodes.

        We're defining a logjam as a
        node whose input queue is full, but whose output queue(s) is not.
        More specifically, we poll each node in the ``monitor_thread``,
        and increment a counter if the node is a logjam at that time. This
        property returns the percentage of samples in which the node is a
        logjam. Our intention is that if this score exceeds a threshold,
        the user is alerted, or the load is rebalanced somehow (not yet
        implemented).

        Returns:
           (float): Logjam score
        '''

        if self.logjam_score['polled'] == 0:
            return 0.
        else:
            return self.logjam_score[
                'logjam'] / self.logjam_score['polled']

    def global_start(
            self, datadog=False, prometheus=False, pipeline_name=None):
        '''
        Starts every node connected to ``self``. Mainly, it:

        1. calls ``start()`` on each node
        #. sets some global variables
        #. optionally starts some experimental code for monitoring
        '''

        def prometheus_init():
            '''
            Experimental code for enabling Prometheus monitoring.
            '''
            from prometheus_client import (
                start_http_server, Summary, Gauge, Histogram, Counter)
            for node in self.all_connected():
                node.prometheus_objects = {}
                summary = Summary(node.name + '_incoming', 'Summary of incoming messages')
                node.prometheus_objects['incoming_message_summary'] = summary
                node.prometheus_objects['outgoing_message_summary'] = Gauge(
                    node.name + '_outgoing', 'Summary of outgoing messages')
            start_http_server(8000)

        if PROMETHEUS:
            prometheus_init()

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

        run_id = uuid.uuid4().hex
        for node in self.all_connected():
            # Set the pipeline name on the attribute of each node
            node.pipeline_name = pipeline_name or uuid.uuid4().hex
            node.datadog_stats = datadog_stats
            # Set a unique run_id
            node.run_id = run_id
            # Tell each node whether they're logging to datadog
            node.datadog = datadog
            node.global_dict = global_dict  # Establishing shared globals
            logging.debug('global_start:' + str(self))
            thread = threading.Thread(target=NanoNode.stream, args=(node, ), daemon=True)
            thread.start()
            node.thread_dict = self.thread_dict
            self.thread_dict[node.name] = thread
        monitor_thread = threading.Thread(
            target=NanoNode.thread_monitor, args=(self,), daemon=False)
        monitor_thread.start()

    @property
    def input_queue_size(self):
        '''
        Return the total number of items in all of the queues that are inputs
        to this node.
        '''

        return sum(
            [input_queue.queue.qsize() for input_queue in self.input_queue_list])

    def kill_pipeline(self):
        for node in self.all_connected():
            node.finished = True

    def draw_pipeline(self):
        '''
        Draw the pipeline structure using graphviz.
        '''

        dot = graphviz.Digraph()
        for node in self.all_connected():
            dot.node(node.name, node.name, shape='box')
        for node in self.all_connected():
            for target_node in node.output_node_list:
                dot.edge(node.name, target_node.name)
        dot.render('pipeline_drawing.gv', view=True)

    def thread_monitor(self):
        '''
        This function loops over all of the threads in the pipeline, checking
        that they are either ``finished`` or ``running``. If any have had an
        abnormal exit, terminate the entire pipeline.
        '''

        counter = 0
        pipeline_finished = all(
            node.finished for node in self.all_connected())
        error = False
        while not pipeline_finished:
            logging.debug('MONITOR THREAD')
            time.sleep(MONITOR_INTERVAL)
            counter += 1

            # Check whether all the workers have ``.finished``
            # pipeline_finished = all(
            #     node.finished for node in self.all_connected())

            if counter % STATS_COUNTER_MODULO == 0:
                table = prettytable.PrettyTable(
                    ['Node', 'Class', 'Received', 'Sent',
                     'Queued', 'Status', 'Time'])
                for node in sorted(
                        list(self.all_connected()), key=lambda x: x.name):
                    if node.status == 'running':
                        status_color = bcolors.WARNING
                    elif node.status == 'stopped':
                        status_color = ''
                    elif node.status == 'error':
                        status_color = bcolors.FAIL
                        error = True
                    elif node.status == 'success':
                        status_color = bcolors.OKGREEN
                    else:
                        assert False

                    if node.logjam >= LOGJAM_THRESHOLD:
                        logjam_color = bcolors.FAIL
                    else:
                        logjam_color = ''

                    table.add_row(
                        [
                            logjam_color + node.name + bcolors.ENDC,
                            node.__class__.__name__,
                            node.messages_received_counter,
                            node.messages_sent_counter,
                            node.input_queue_size,
                            status_color + node.status + bcolors.ENDC,
                            node.time_running])
                logging.info('\n' + str(table))
                if error:
                    logging.error('Terminating due to error.')
                    self.terminate_pipeline(error=True)
                    pipeline_finished = True
                    break

            pipeline_finished = all(
                node.finished for node in self.all_connected())

            # Check for blocked nodes
            for node in self.all_connected():
                input_queue_full = [
                    input_queue.approximately_full() for input_queue in
                    node.input_queue_list]

                output_queue_full = [
                    output_queue.approximately_full() for output_queue in
                    node.output_queue_list]

                logjam = (
                    not node.is_source and all(input_queue_full)
                    and not any(output_queue_full))
                node.logjam_score['polled'] += 1
                logging.debug('LOGJAM SCORE: {logjam}'.format(
                    logjam=str(node.logjam)))
                if logjam:
                    node.logjam_score['logjam'] += 1
                logging.debug('LOGJAM {logjam} {name}'.format(
                    logjam=logjam, name=node.name))

        logging.info('Pipeline finished.')
        logging.info('Sending terminate signal to nodes.')
        logging.info('Messages that are being processed will complete.')

        if error:
            sys.exit(1)
        else:
            sys.exit(0)


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


class InsertData(NanoNode):

    def __init__(self, overwrite=True, overwrite_if_null=True, value_dict=None, **kwargs):
        self.overwrite = overwrite
        self.overwrite_if_null = overwrite_if_null
        self.value_dict = value_dict or {}
        super(InsertData, self).__init__(**kwargs)

    def process_item(self):
        logging.debug('INSERT DATA: ' + str(self.__message__))
        for key, value in self.value_dict.items():
            if (key not in self.__message__) or self.overwrite or (
                self.__message__.get(key) == None and
                     self.overwrite_if_null):
                self.__message__[key] = value
        yield self.__message__


class RandomSample(NanoNode):
    '''
    Lets through only a random sample of incoming messages. Might be useful
    for testing, or when only approximate results are necessary.
    '''

    def __init__(self, sample=.1):
        self.sample = sample

    def process_item(self):
        yield self.message if random.random() <= self.sample else None


class SubstituteRegex(NanoNode):
    def __init__(self, match_regex=None, substitute_string=None, *args, **kwargs):
        self.match_regex = match_regex
        self.substitute_string = substitute_string
        self.regex_obj = re.compile(self.match_regex)
        super(SubstituteRegex, self).__init__(*args, **kwargs)

    def process_item(self):
        out = self.regex_obj.sub(self.substitute_string, self.message[self.key])
        yield out


class CSVToDictionaryList(NanoNode):
    def __init__(self, **kwargs):
        super(CSVToDictionaryList, self).__init__(**kwargs)

    def process_item(self):
        csv_file_obj = io.StringIO(self.__message__)
        csv_reader = csv.DictReader(csv_file_obj)
        output = [row for row in csv_reader]
        yield output


class SequenceEmitter(NanoNode):
    '''
    Emits ``sequence`` ``max_sequences`` times, or forever if
    ``max_sequences`` is ``None``.
    '''

    def __init__(self, sequence, *args, max_sequences=1, **kwargs):
        self.sequence = sequence
        self.max_sequences = max_sequences
        super(SequenceEmitter, self).__init__(*args, **kwargs)

    def generator(self):
        '''
        Emit the sequence ``max_sequences`` times.
        '''
        type_dict = {
            'int': int,
            'integer': int,
            'str': str,
            'string': str,
            'float': float,
            'bool': to_bool}
        counter = 0
        while counter < self.max_sequences:
            for item in self.sequence:
                if isinstance(item, (dict,)) and 'value' in item and 'type' in item:
                    item = type_dict[item['type'].lower()](item['value'])
                item = {self.output_key: item}
                yield item
            counter += 1

    def process_item(self):
        '''
        Emit the sequence ``max_sequences`` times.
        '''
        type_dict = {
            'int': int,
            'integer': int,
            'str': str,
            'string': str,
            'float': float,
            'bool': to_bool}
        counter = 0
        while counter < self.max_sequences:
            for item in self.sequence:
                if isinstance(item, (dict,)) and 'value' in item and 'type' in item:
                    item = type_dict[item['type'].lower()](item['value'])
                item = {self.output_key: item}
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


class SimpleTransforms(NanoNode):
    def __init__(self, missing_keypath_action='ignore', starting_path=None,
            transform_mapping=None, target_value=None, keypath=None, **kwargs):

        self.missing_keypath_action = missing_keypath_action
        self.transform_mapping = transform_mapping or []
        self.functions_dict = {}
        self.starting_path = starting_path

        for transform in self.transform_mapping:
            # Not doing the transforms; only loading the right functions here
            function_name = transform.get('target_function', None)
            full_function_name = function_name
            if function_name is not None:
                components = function_name.split('__')
                if len(components) == 1:
                    module = None
                    function_name = components[0]
                    function_obj = globals()[function_name]
                else:
                    module = '.'.join(components[:-1])
                    function_name = components[-1]
                    module = importlib.import_module(module)
                    function = getattr(module, function_name)
                self.functions_dict[full_function_name] = function

        super(SimpleTransforms, self).__init__(**kwargs)

    def process_item(self):
        logging.debug('TRANSFORM ' + str(self.name))
        logging.debug(self.name + ' ' + str(self.message))
        for transform in self.transform_mapping:
            path = transform['path']
            target_value = transform.get('target_value', None)
            function_name = transform.get('target_function', None)
            starting_path = transform.get('starting_path', None)
            if function_name is not None:
                function = self.functions_dict[function_name]
            else:
                function = None
            function_kwargs = transform.get('function_kwargs', None)
            function_args = transform.get('function_args', None)
            logging.debug(self.name + ' calling replace_by_path:')
            replace_by_path(
                self.message,
                tuple(path),
                target_value=target_value,
                function=function,
                function_args=function_args,
                starting_path=starting_path,
                function_kwargs=function_kwargs)
            logging.debug(
                'after SimpleTransform: ' + self.name + str(self.message))
        yield self.message


class Serializer(NanoNode):
    '''
    Takes an iterable thing as input, and successively yields its items.
    '''

    def __init__(self, values=False, *args, **kwargs):
        self.values = values
        super(Serializer, self).__init__(**kwargs)

    def process_item(self):
        if self.__message__ is None:
            yield None
        elif self.values:
            for item in self.__message__.values():
                yield item
        else:
            for item in self.__message__:
                logging.debug(self.name + ' ' + str(item))
                yield item


class AggregateValues(NanoNode):
    '''
    Does that.
    '''

    def __init__(self, values=False, tail_path=None, **kwargs):
        self.tail_path = tail_path
        self.values = values
        super(AggregateValues, self).__init__(**kwargs)

    def process_item(self):
        values = aggregate_values(
            self.__message__, self.tail_path, values=self.values)
        logging.debug('aggregate_values ' + self.name + ' ' + str(values))
        yield values


class Filter(NanoNode):
    '''
    Applies tests to each message and filters out messages that don't pass

    Built-in tests:
        key_exists
        value_is_true
        value_is_not_none

    Example:
        {'test': 'key_exists',
         'key': mykey}

    '''
    def __init__(
            self, test=None, test_keypath=None, value=True, *args, **kwargs):
        self.test = test
        self.value = value
        self.test_keypath = test_keypath or []
        super(Filter, self).__init__(*args, **kwargs)

    @staticmethod
    def _key_exists(message, key):
        return key in message

    @staticmethod
    def _value_is_not_none(message, key):
        logging.debug('value_is_not_none: {message} {key}'.format(
            message=str(message), key=key))
        return get_value(message, key) is not None

    @staticmethod
    def _value_is_true(message, key):
        return to_bool(message.get(key, False))

    def process_item(self):
        if self.test in ['key_exists', 'value_is_not_none', 'value_is_true']:
            result = (
                getattr(self, '_' + self.test)(
                    self.__message__, self.test_keypath) == self.value)
        else:
            raise Exception('Unknown test: {test_name}'.format(
                test_name=test))
        if result:
            logging.debug('Sending message through')
            yield self.message
        else:
            logging.debug('Blocking message: ' + str(self.__message__))
            yield NothingToSeeHere()


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
    def __init__(self, disable=False, pretty=False,
            prepend='printer: ', **kwargs):
        self.disable = disable
        self.pretty = pretty
        super(PrinterOfThings, self).__init__(**kwargs)
        logging.debug('Initialized printer...')

    def process_item(self):
        if not self.disable:
            print(self.prepend)
            if self.pretty:
                pprint.pprint(self.__message__, indent=2)
            else:
                print(str(self.__message__))
            print('\n')
            print('------------')
        yield self.message


class ConstantEmitter(NanoNode):
    '''
    Send a thing every n seconds
    '''

    def __init__(self, thing=None, delay=2, **kwargs):
        self.thing = thing
        self.delay = delay
        super(ConstantEmitter, self).__init__(**kwargs)

    def generator(self):
        while 1:
            if random.random() < -.1:
                assert False
            time.sleep(self.delay)
            yield self.thing


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
        value_to_match = get_value(
            self.message, self.stream_paths[self.message_source.name])
        # Check for matches in all other streams.
        # If complete set of matches, yield the merged result
        # If not, add it to the `TimedDict`.
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
        cls = template_class(
            node_name, _class, node_config['remapping'],
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
        logging.debug(
            'Remapper {node}:'.format(node=self.name) + str(self.__message__))
        out = remap_dictionary(self.__message__, self.remapping_dict)
        yield out


class BatchMessages(NanoNode):
    def __init__(
        self, batch_size=None, batch_list=None,
            counter=0, timeout=5, **kwargs):
        self.batch_size = batch_size
        self.timeout = timeout
        self.counter = 0
        self.batch_list = batch_list or []
        super(BatchMessages, self).__init__(**kwargs)

    def process_item(self):
        self.counter += 1
        self.batch_list.append(self.__message__)
        logging.debug(self.name + ' ' + str(self.__message__))
        out = NothingToSeeHere()
        if self.counter % self.batch_size == 0:
            out = self.batch_list
            logging.debug('BatchMessages: ' + str(out))
            self.batch_list = []
        yield out

    def cleanup(self):
        yield self.batch_list


if __name__ == '__main__':
    pass

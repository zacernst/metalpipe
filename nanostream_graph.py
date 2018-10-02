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

import networkx as nx
import types
import yaml
import logging
import threading
import functools
import time
import sys
import multiprocessing as mp
import nanostream_processor
import inspect
from nanostream_node_classes import (
    StringSplitter, Serializer, PrinterOfThings, ConstantEmitter)


logging.basicConfig(level=logging.DEBUG)


class NanoStreamGraph(object):
    """
    They're actually directed graphs.
    """
    def __init__(self, **kwargs):
        self.graph = nx.DiGraph()
        self.node_list = []  # nodes are listeners, processors, etc.
        self.edge_list = []  # edges are queues
        self.thread_list = []  # We'll add these when `start` is called
        self.thread_constructor = threading.Thread  # For future mp support
        self.global_dict = {key: value for key, value in kwargs.items()}
        self.node_dict = {}

    def add_node(self, node):
        self.node_list.append(node)
        self.graph.add_node(node)
        node.global_dict = self.global_dict
        node.graph = self

        if node.name not in self.node_dict:
            self.node_dict[node.name] = node
        else:
            logging.warning('same name used for two nodes.')
        # Recurse into node if the node is a dynamic set of nodes
        if isinstance(node, DynamicClassMediator):
            for _, node_dict in node.node_dict.items():
                node_obj = node_dict['obj']
                self.add_node(node_obj)

    def __getattribute__(self, attr):
        '''
        Allow us to access `NanoStreamProcessor` nodes as attributes.
        '''
        if attr in super(
                NanoStreamGraph, self).__getattribute__('node_dict'):
            return super(
                NanoStreamGraph, self).\
                __getattribute__('node_dict')[attr]
        else:
            return super(NanoStreamGraph, self).__getattribute__(attr)

    def __add__(self, other):
        self.add_node(other)
        return self

    @property
    def sources(self):
        return [node for node in self.node_list if node.is_source]

    @property
    def sinks(self):
        return [node for node in self.node_list if node.is_sink]

    @property
    def number_of_sources(self):
        return len(self.sources)

    @property
    def number_of_sinks(self):
        return len(number_of_sinks)

    def start(self, block=False):
        """
        We check whether any of the nodes have a "run_on_start" function.
        """
        for node_name, node in self.node_dict.items():
            if hasattr(node, 'run_on_start'):
                logging.info(
                    'Found `run_on_start` for: {node_name}'.format(
                        node_name=node_name))
                node.run_on_start()
        for node_name, node in self.node_dict.items():
            logging.info('Starting node {node_name}'.format(
                node_name=node_name))
            worker = self.thread_constructor(target=node.start)
            self.thread_list.append(worker)
            worker.start()
            logging.info('Started node {node_name}'.format(
                node_name=node_name))
        monitor_thread = threading.Thread(
            target=NanoStreamGraph.monitor_nodes, args=(self,))
        monitor_thread.start()

    def monitor_nodes(self):
        '''
        Just logs dead threads. Need to do more.
        '''
        while 1:
            for node_thread in self.thread_list:
                if not node_thread.isAlive():
                    logging.error('Dead thread')
            time.sleep(1)


class NanoGraphWorker(object):
    """
    Not so sure this is useful anymore.

    Subclass this, and override the `worker` method. Call `add_worker`
    on the `NanoStreamGraph` object.
    """
    def __init__(self):
        pass

    def worker(self, *args, **kwargs):
        raise NotImplementedError("Need to override worker method")


def get_config_file(pathname):
    config = yaml.load(open(pathname, 'r').read())
    return config


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
    parameters = [i for i, _ in list(inspect.signature(f).parameters.items())]
    for kwarg in parameters:
        if kwarg not in kwarg_mapping:
            reverse_mapping[kwarg] = kwarg

    def remapped_function(*args, **kwargs):
        remapped_kwargs = {
            reverse_mapping[argument]: value for argument, value
            in kwargs.items() if argument in parameters}
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


def foo():
    return 'hi'


class DynamicClassMediator(nanostream_processor.NanoStreamProcessor):

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

    def start(self):
        for node_dict in self.node_dict.values():
            print(node_dict)
            logging.info('Starting node: {node}'.format(node='hi'))
            node_dict['obj'].start()

    def hi(self):
        return 'hi'


class ProcessorClassFactory(type):

    def __new__(cls, raw_config):
        new_class = super().__new__(
            cls, raw_config['name'], (DynamicClassMediator,), {})
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
    raw_config = get_config_file('compose.yaml')
    encapsulator = ProcessorClassFactory(raw_config)
    obj = encapsulator(outer_delimiter=',')
    # sys.exit(0)
    printer = PrinterOfThings()
    emitter = ConstantEmitter(thing='foo,bar')
    graph = NanoStreamGraph()
    graph.add_node(obj)
    graph.add_node(printer)
    graph.add_node(emitter)
    emitter > obj
    obj > printer
    graph.start()

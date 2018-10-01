import logging
import yaml
import inspect
import functools
import types

from nanostream_node_classes import (
    PrinterOfThings, ConstantEmitter, HttpGetRequest, HttpPostRequest,
    StringSplitter, Serializer)
from nanostream_processor import NanoStreamProcessor
from nanostream_graph import NanoStreamGraph


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
    for kwarg, _ in list(inspect.signature(f).parameters.items()):
        if kwarg not in kwarg_mapping:
            reverse_mapping[kwarg] = kwarg

    def remapped_function(*args, **kwargs):
        remapped_kwargs = {
            reverse_mapping[argument]: value for argument, value in kwargs.items()}
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


class DynamicClassMediator(NanoStreamProcessor):
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

    def get_sink(self):
        sinks = self.sink_list()
        if len(sinks) != 1:
            raise Exception(
                '`DynamicClassMediator` needs to have exactly one sink.')
        return sinks[0]

    def get_source(self):
        sources = self.source_list()
        if len(sources) != 1:
            raise Exception(
                '`DynamicClassMediator` needs to have exactly one source.')
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



    # instantiations of dynamically created classes have no methods.
    # a.barFighters = types.MethodType( barFighters, a )




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
    obj = encapsulator()


    # Steps:
    # 1.
    '''
    Upon instantiating `SplitAndSerialize`, the class must:
    * Instantiate the templated classes, sending the kwargs of the
      `__init__` function to the right classes.
    * Construct the edge queues between the right node object.
    * Point its own source queue to the input queue of the root node.
    * Point its own sink queue to the output queue of the leaf node.
    * Its `start` method must loop over all the nodes and start them.
    '''

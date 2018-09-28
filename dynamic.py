import logging
import yaml
import inspect
import functools
from types import FunctionType

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
        node_dict[node_name]['obj'] = node_obj
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


class ProcessorClassFactory(type):

    def __new__(cls, raw_config):
        new_class = super().__new__(
            cls, raw_config['name'], (NanoStreamProcessor,), {})
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

    # Steps:
    # 1.

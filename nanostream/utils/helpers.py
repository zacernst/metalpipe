'''
Helper module
*************

Misc. helper functions for other classes.
'''

import copy
import time
import logging


def to_bool(thing):
    if isinstance(thing, (str,)):
        value = len(thing) > 0 and thing[0].lower() in ['t', 'y']
    elif isinstance(thing, (int, float,)):
        return thing > 0
    elif isinstance(thing, (bool,)):
        return thing
    else:
        raise Exception(
            'Do not know how to convert {thing} to bool'.format(
                thing=str(thing)))


def get_value(
    dictionary, path, delimiter='.',
        use_default_value=False, default_value=None):

    dictionary = copy.deepcopy(dictionary)
    logging.debug(dictionary)
    logging.debug(path)
    if isinstance(path, (str,)):
        path = path.split(delimiter)
    elif isinstance(path, (list, tuple,)):
        pass
    else:
        raise Exception('what?')
    if len(path) == 0:
        dictionary = dictionary
    else:
        for step in path:
            if dictionary is None or isinstance(dictionary, (dict,)):
                dictionary = (dictionary or {}).get(step, default_value)
            else:
                dictionary = dictionary[step.index]
    return dictionary


def hi(*args, **kwargs):
    return 'hi'


def set_value(dictionary, path, value):
    for step in path[:-1]:
        if not isinstance(step, (ListIndex,)):
            if step not in dictionary:
                break
            dictionary = dictionary[step]
        else:
            dictionary = dictionary[step.index]
    dictionary[path[-1]] = value


def iterate_leaves(dictionary, keypath=None):
    keypath = keypath or []
    for key, value in dictionary.items():
        if not isinstance(value, (dict,)):
            yield keypath + [key], value
        else:
            for i in iterate_leaves(value, keypath=keypath + [key]):
                yield i


def remap_dictionary(
    source_dictionary, target_dictionary,
        use_default_value=False, default_value=None):
    target_dictionary = copy.deepcopy(target_dictionary)
    for path, value in iterate_leaves(target_dictionary):
        set_value(
            target_dictionary, path, get_value(source_dictionary, value, use_default_value=use_default_value, default_value=default_value))
    return target_dictionary


def now_milliseconds():
    return str(int(time.time() * 1000))


def two_weeks_ago():
    return str(int(time.time() * 1000 - (14 * (24 * 60 * 60 * 1000))))


class SafeMap(dict):
    def __missing__(self, key):
        return '{' + str(key) + '}'


class ListIndex:
    def __init__(self, index):
        self.index = index

    def __repr__(self):
        return 'ListIndex({index})'.format(index=str(self.index))

    def __eq__(self, other):
        return isinstance(other, (ListIndex,)) and self.index == other.index

    def __hash__(self):
        return self.index


def all_paths(thing, path=None):

    path = path or tuple()
    if isinstance(thing, (dict,)):
        for key, value in thing.items():
            yield path + (key,)
            for i in all_paths(value, path=path + (key,)):
                yield i

    elif isinstance(thing, (list,)):
        for index, item in enumerate(thing):
            yield path + (ListIndex(index),)
            for i in all_paths(item, path=path + (ListIndex(index),)):
                yield i
    else:
        yield path


def matching_tail_paths(target_path, structure):
    seen = set()
    for path in all_paths(structure):
        if path in seen:
            continue
        if isinstance(path[-1], (ListIndex,)):
            continue
        seen.add(path)
        temp_list = tuple(
            step for step in path if not isinstance(step, (ListIndex,)))
        if len(temp_list) < len(target_path):
            continue
        if temp_list[-1 * len(target_path):] == target_path:
            yield path


def replace_by_path(
    dictionary, target_path, target_value=None, function=None,
        function_args=None, function_kwargs=None):

    function = function or (lambda x: x)
    function_args = function_args or tuple([])
    function_kwargs = function_kwargs or {}
    dictionary_clone = copy.deepcopy(dictionary)
    for path in matching_tail_paths(target_path, dictionary_clone):
        current_value = get_value(dictionary, path)
        target_value = target_value or function(
            current_value, *function_args, **function_kwargs)
        set_value(dictionary, path, target_value)


if __name__ == '__main__':
    d = {
        'foo': 'bar',
        'bar': 'baz',
        'baz': 'qux',
        'foobar': [1, {'hi': 'there'}, 3, {'hi': 'dude'}]}
    import json
    d = json.load(open('./sample.json', 'r'))

    target_path = ('contacts', 'addedAt')

    replace_by_path(d, target_path, target_value='hithere')
    print(d)

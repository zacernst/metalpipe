'''
Helper module
*************

Misc. helper functions for other classes.
'''

import copy
import time
import logging
import datetime
import pickle
import hashlib
import uuid


def list_to_dict(some_list, list_of_keys):
    if len(some_list) != len(list_of_keys):
        raise Exception('Length of list elements and key list must be equal.')
    out = {
        list_of_keys[index]: item for index, item in enumerate(some_list)}
    return out


def to_bool(thing):
    if isinstance(thing, (str,)):
        return len(thing) > 0 and thing[0].lower() in ['t', 'y']
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


def pdb(*args, **kwargs):
    import pdb; pdb.set_trace()


def convert_date_format(date_string, source_format=None, target_format=None):
    return datetime.datetime.strptime(
        date_string, source_format).strftime(target_format)


def engaging_networks_date(result, **kwargs):
    '''
    This is a very redundant function, but it's here mostly to test the
    functionality of the ``post_process_function``.
    '''
    out = datetime.datetime.strptime(result[0]['date_modified'], '%Y-%m-%d')
    out = out.strftime('%Y%m%d')
    return out


def set_value(dictionary, path, value):
    for step in path[:-1]:
        if not isinstance(step, (ListIndex,)):
            if step not in dictionary:
                raise Exception('Path not found.')
            dictionary = dictionary[step]
        else:
            dictionary = dictionary[step.index]
    dictionary[
        path[-1] if not isinstance(path[-1], (ListIndex,))
        else path[-1].index] = value


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
            target_dictionary, path,
            get_value(
                source_dictionary,
                value,
                use_default_value=use_default_value,
                default_value=default_value))
    return target_dictionary


def now_milliseconds():
    return str(int(time.time() * 1000))


def two_weeks_ago():
    return str(int(time.time() * 1000 - (4 * (24 * 60 * 60 * 1000))))


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
    target_path = tuple(target_path)
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
        tail_of_path = temp_list[-1 * len(target_path):]
        if tail_of_path == target_path:
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


def aggregate_values(dictionary, target_path, values=False):
    aggregated_values = []
    for path in matching_tail_paths(target_path, dictionary):
        current_value = get_value(dictionary, path)
        aggregated_values.append(list(current_value.values()) if values else current_value)
    logging.debug('aggregated_values: ' + str(aggregated_values))
    return aggregated_values if not values else aggregated_values[0]


class UberDict(dict):

    def rget(self, value):
        for i in meets_condition(self, lambda x: isinstance(x, (dict,))):
            for _key, _value in i.items():
                if _value == value:
                    yield _key


def iterate(thing, path=None, seen=None):
    # json.dumps(d, sort_keys=True)
    hashed_thing = hash(str(thing))
    # json.dumps(thing, sort_keys=True))
    # hashlib.md5(pickle.dumps(thing)).hexdigest()
    path = path or []
    seen = seen or set([])
    if hashed_thing not in seen:
        if isinstance(thing, (list, tuple, dict,)):
            yield thing
        seen.add(hashed_thing)
        if isinstance(thing, (dict,)):
            for key, value in thing.items():
                for item in iterate(value, path=path + [key], seen=seen):
                    yield item
        elif isinstance(thing, (list, tuple,)):
            for index, item in enumerate(thing):
                for thingie in iterate(item, path=path + [ListIndex(index)], seen=seen):
                    yield thingie
        else:
            yield thing


def meets_condition(thing, func):
    for item in iterate(thing):
        if func(item):
            yield item


if __name__ == '__main__':
    d = {
        '1': '2',
        '3': '4',
        '5': '6',
        '7': ['8', {'9': '10'}, '11', {'12': '13'}]}

    d = UberDict(d)

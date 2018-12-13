'''
Helper module
*************

Misc. helper functions for other classes.
'''
import copy


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
    target_dictionary = copy.deepcopy(target_dictionary)
    for path, value in iterate_leaves(target_dictionary):
        set_value(
            target_dictionary, path, get_value(source_dictionary, value))
    return target_dictionary


class SafeMap(dict):
    def __missing__(self, key):
        return '{' + str(key) + '}'


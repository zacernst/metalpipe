"""
Helper module
*************

Misc. helper functions for other classes.
"""

import copy
import types
import time
import logging
import importlib
import datetime
import pickle
import hashlib
import uuid
import pickle
import base64


UNIX_EPOCH = datetime.datetime(month=1, year=1970, day=1)


def package(thing):
    return base64.b64encode(pickle.dumps(thing))


def unpackage(thing):
    return pickle.loads(base64.b64decode(thing))


def list_to_dict(some_list, list_of_keys):
    if len(some_list) != len(list_of_keys):
        raise Exception("Length of list elements and key list must be equal.")
    out = {list_of_keys[index]: item for index, item in enumerate(some_list)}
    return out


def load_function(function_name):
    components = function_name.split("__")
    module = ".".join(components[:-1])
    function_name = components[-1]
    module = importlib.import_module(module)
    function = getattr(module, function_name)
    return function


def timestamp_to_redshift(timestamp):
    if isinstance(timestamp, (str,)):
        return timestamp
    return timestamp.strftime("%b %d,%Y  %H:%M:%S")


def string_to_redshift(timestamp):
    """
    TODO: Consider removing this function.
    """
    if timestamp is None or (
        isinstance(timestamp, (str,)) and timestamp == ""
    ):
        return timestamp
    return datetime.datetime.strptime(timestamp, "%b %d,%Y  %H:%M:%S")


def string_to_datetime(timestamp):
    if timestamp is None or (
        isinstance(timestamp, (str,)) and timestamp == ""
    ):
        return timestamp
    return datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")


def milliseconds_epoch_to_datetime(milliseconds_epoch):
    if isinstance(milliseconds_epoch, (datetime.datetime,)):
        return milliseconds_epoch
    try:
        milliseconds_epoch = int(milliseconds_epoch)
    except:
        logging.info(
            "milliseconds exception: " + str(type(milliseconds_epoch))
        )
        return milliseconds_epoch
    logging.debug("milliseconds_epoch_to_datetime: " + str(milliseconds_epoch))
    out = UNIX_EPOCH + datetime.timedelta(
        seconds=(int(milliseconds_epoch) / 1000)
    )
    logging.debug("milliseconds_epoch_to_datetime output: " + str(out))
    return out


def seconds_epoch_to_datetime(seconds_epoch):
    return UNIX_EPOCH + datetime.timedelta(seconds=(seconds))


def to_bool(thing):
    if isinstance(thing, (str,)):
        return len(thing) > 0 and thing[0].lower() in ["t", "y"]
    elif isinstance(thing, (int, float)):
        return thing > 0
    elif isinstance(thing, (bool,)):
        return thing
    else:
        raise Exception(
            "Do not know how to convert {thing} to bool".format(
                thing=str(thing)
            )
        )


def get_value(
    dictionary,
    path,
    delimiter=".",
    use_default_value=False,
    default_value=None,
):

    # dictionary = copy.deepcopy(dictionary)
    if isinstance(path, (str,)):
        path = path.split(delimiter)
    elif isinstance(path, (list, tuple)):
        pass
    else:
        raise Exception("what?")
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
    return "hi"


def pdb(*args, **kwargs):
    import pdb

    pdb.set_trace()


def convert_date_format(date_string, source_format=None, target_format=None):
    return datetime.datetime.strptime(date_string, source_format).strftime(
        target_format
    )


def engaging_networks_date(result, **kwargs):
    """
    This is a very redundant function, but it's here mostly to test the
    functionality of the ``post_process_function``.
    """
    out = datetime.datetime.strptime(result[0]["date_modified"], "%Y-%m-%d")
    out = out.strftime("%Y%m%d")
    return out


def set_value(dictionary, path, value):
    for step in path[:-1]:
        if not isinstance(step, (ListIndex,)):
            if step not in dictionary:
                raise Exception("Path not found.")
            dictionary = dictionary[step]
        else:
            dictionary = dictionary[step.index]
    dictionary[
        path[-1] if not isinstance(path[-1], (ListIndex,)) else path[-1].index
    ] = value


def iterate_leaves(dictionary, keypath=None):
    keypath = keypath or []
    for key, value in dictionary.items():
        if not isinstance(value, (dict,)):
            yield keypath + [key], value
        else:
            for i in iterate_leaves(value, keypath=keypath + [key]):
                yield i


def remap_dictionary(
    source_dictionary,
    target_dictionary,
    use_default_value=False,
    default_value=None,
):
    target_dictionary = copy.deepcopy(target_dictionary)
    for path, value in iterate_leaves(target_dictionary):
        set_value(
            target_dictionary,
            path,
            get_value(
                source_dictionary,
                value,
                use_default_value=use_default_value,
                default_value=default_value,
            ),
        )
    return target_dictionary


def now_milliseconds():
    return str(int(time.time() * 1000))


def two_weeks_ago():
    return (
        datetime.datetime(year=2019, month=1, day=1) - UNIX_EPOCH
    ).total_seconds() * 1000
    # return str(int(time.time() * 1000 - (30 * (24 * 60 * 60 * 1000))))


def now_redshift():
    return datetime.datetime.now().strftime("%b %d,%Y  %H:%M:%S")


def now_datetime():
    return datetime.datetime.now()


def two_weeks_ago_datetime():
    return datetime.datetime.now() - datetime.timedelta(days=14)


def january_1_2019():
    return datetime.datetime(year=2016, month=1, day=1)


def datetime_to_redshift(datetime_obj):
    return datetime_obj.strftime("%b %d,%Y  %H:%M:%S")


def datetime_to_milliseconds(datetime_obj):
    return int((datetime_obj - UNIX_EPOCH).total_seconds() * 1000)


class SafeMap(dict):
    def __missing__(self, key):
        return "{" + str(key) + "}"


class ListIndex:
    def __init__(self, index):
        self.index = index

    def __repr__(self):
        return "ListIndex({index})".format(index=str(self.index))

    def __eq__(self, other):
        return isinstance(other, (ListIndex,)) and self.index == other.index

    def __hash__(self):
        return self.index


def all_paths(thing, path=None, starting_path=None):
    path = path or tuple()
    starting_path = starting_path or []
    thing = get_value(thing, starting_path)

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


def last_element_list_index(path):
    return isinstance(path[-1], (ListIndex,))


def remove_list_indexes(path):
    return tuple(step for step in path if not isinstance(step, (ListIndex,)))


def matching_tail_paths(target_path, structure, starting_path=None):
    target_path = tuple(target_path)
    if starting_path is not None:
        starting_path = tuple(starting_path)
    seen = set()
    for path in all_paths(structure, starting_path=starting_path):
        if path in seen:
            continue
        elif len(path) == 0:
            continue
        elif last_element_list_index(path):
            continue
        else:
            seen.add(path)

        temp_list = remove_list_indexes(path)

        if len(temp_list) < len(target_path):
            continue
        tail_of_path = temp_list[-1 * len(target_path) :]
        if lists_equal(tail_of_path, target_path):
            logging.debug("tail path: " + str(target_path) + " " + str(path))
            yield (starting_path or tuple([])) + path


def lists_equal(l1, l2):
    return l1 == l2


def nevermind(thing):
    return thing


def replace_by_path(
    dictionary,
    target_path,
    target_value=None,
    function=None,
    function_args=None,
    function_kwargs=None,
    starting_path=None,
):

    function = function or nevermind
    function_args = function_args or tuple([])
    function_kwargs = function_kwargs or {}
    # dictionary_clone = copy.deepcopy(dictionary)
    dictionary_clone = dictionary
    for path in matching_tail_paths(
        target_path, dictionary_clone, starting_path=starting_path
    ):
        current_value = get_value(dictionary, path)
        one_target_value = target_value or function(
            current_value, *function_args, **function_kwargs
        )
        set_value(dictionary, path, one_target_value)


def aggregate_values(dictionary, target_path, values=False):
    aggregated_values = []
    for path in matching_tail_paths(target_path, dictionary):
        current_value = get_value(dictionary, path)
        try:
            aggregated_values.append(
                list(current_value.values()) if values else current_value
            )
        except AttributeError:
            logging.debug(
                "Attribute error in aggregate values. No list returned."
            )

    logging.debug("aggregated_values: " + str(aggregated_values))
    try:
        out = aggregated_values if not values else aggregated_values[0]
    except IndexError:
        out = []
    return out


class UberDict(dict):
    def rget(self, value):
        for i in meets_condition(self, lambda x: isinstance(x, (dict,))):
            for _key, _value in i.items():
                if _value == value:
                    yield _key


def iterate(thing, path=None, seen=None, start_path=None):
    """
    TODO: allow specification of `start_path` to speed this up.
    """
    hashed_thing = hash(str(thing))
    path = path or []
    start_path = start_path or []
    seen = seen or set([])
    if hashed_thing not in seen:
        if isinstance(thing, (list, tuple, dict)):
            yield thing
        seen.add(hashed_thing)
        if isinstance(thing, (dict,)):
            for key, value in thing.items():
                for item in iterate(value, path=path + [key], seen=seen):
                    yield item
        elif isinstance(thing, (list, tuple)):
            for index, item in enumerate(thing):
                for thingie in iterate(
                    item, path=path + [ListIndex(index)], seen=seen
                ):
                    yield thingie
        else:
            yield thing


def meets_condition(thing, func):
    for item in iterate(thing):
        if func(item):
            yield item


if __name__ == "__main__":
    import json

    d = json.load(open("./sample.json", "r"))
    counter = 0
    while counter < 100:
        replace_by_path(d, ["vid"], target_value="hithere")
        counter += 1
    import random

    logging.basicConfig(level=logging.debug)
    replace_by_path(
        d, ["vid"], function=lambda x: str(x) + str(random.random())
    )

import os
import logging
import time

import pytest
from timed_dict.timed_dict import TimedDict
import nanostream.node as node
import nanostream.node_queue.queue as queue
import nanostream.message.poison_pill as poison_pill
from nanostream.utils.required_arguments import (
    MissingRequiredArgument, required_arguments)
from nanostream.utils.set_attributes import set_kwarg_attributes
from nanostream.utils.helpers import (
    aggregate_values, list_to_dict, to_bool, get_value,
    set_value, ListIndex, replace_by_path)


os.environ['PYTHONPATH'] = '.'
logging.basicConfig(level=logging.INFO)


def test_test_sanity():
    assert 1 == 1


@pytest.fixture(scope='function')  # By function because we terminate it
def simple_timed_dict():
    '''
    '''
    d = TimedDict(timeout=3)
    return d


@pytest.fixture(scope='function')
def foo_class_required_argument_foo():

    class Foo:
        @required_arguments('foo')
        def __init__(self, foo='bar', bar='baz'):
            self.foo = foo
            self.bar = bar

    return Foo


def test_can_instantiate_timed_dict(simple_timed_dict):
    simple_timed_dict.stop_sweep()


def test_required_arguments_decorator_passes(foo_class_required_argument_foo):
    foo = foo_class_required_argument_foo(foo='baz')
    assert hasattr(foo, 'foo')
    assert hasattr(foo, 'bar')
    assert foo.foo == 'baz'
    assert foo.bar == 'baz'


def test_required_arguments_decorator_fails(foo_class_required_argument_foo):
    with pytest.raises(expected_exception=MissingRequiredArgument):
        foo = foo_class_required_argument_foo()


@pytest.fixture(scope='function')
def foo_class_set_kwarg_attributes():

    class Foo:
        @set_kwarg_attributes()
        def __init__(self, foo='bar', bar='baz'):
            pass

    return Foo


def test_kwarg_attributes_set_to_defaults(foo_class_set_kwarg_attributes):
    foo = foo_class_set_kwarg_attributes()
    assert hasattr(foo, 'foo')
    assert hasattr(foo, 'bar')
    assert foo.foo == 'bar'
    assert foo.bar == 'baz'


def test_kwarg_attributes_set_to_kwarg(foo_class_set_kwarg_attributes):
    foo = foo_class_set_kwarg_attributes(foo='baz', bar='qux')
    assert hasattr(foo, 'foo')
    assert hasattr(foo, 'bar')
    assert foo.foo == 'baz'
    assert foo.bar == 'qux'


def test_list_to_dict():
    some_list = ['foo', 'bar', 'baz', 'qux']
    list_of_things = ['foo_thing', 'bar_thing', 'baz_thing', 'qux_thing']
    out = list_to_dict(list_of_things, some_list)
    correct = {
        'foo': 'foo_thing',
        'bar': 'bar_thing',
        'baz': 'baz_thing',
        'qux': 'qux_thing'}
    assert out == correct


def test_list_to_dict_error():
    some_list = ['foo', 'bar', 'baz', 'qux', 'oops']
    list_of_things = ['foo_thing', 'bar_thing', 'baz_thing', 'qux_thing']
    with pytest.raises(expected_exception=Exception):
        out = list_to_dict(list_of_things, some_list)


def test_to_bool():
    true_things = ['t', 'T', 'True', 'yes', 'YES', 1, True]
    false_things = ['f', 'F', 'False', 'no', 'NO', 0, False]
    assert all([to_bool(i) for i in true_things])
    assert not any([to_bool(i) for i in false_things])


def test_to_bool_error():
    with pytest.raises(expected_exception=Exception):
        to_bool(['blargh'])


@pytest.fixture(scope='function')
def some_dictionary():

    dictionary = {
        'outer_1': 1,
        'outer_2': 2,
        'outer_3': {'level_2': 3},
        'outer_4': [4, 5, 6],
        'outer_5': [{'inner_1': 'value_1'}, {'inner_2': 'value_2'}]}
    return dictionary

@pytest.fixture(scope='function')
def dict_with_dup_keys():

    dictionary = {
        'outer_1': 1,
        'outer_2': {'level_2': 4},
        'outer_3': {'level_2': 3},
        'outer_4': [4, 5, 6],
        'outer_5': [{'inner_1': 'value_1'}, {'inner_1': 'value_2'}]}
    return dictionary


def test_get_value(some_dictionary):
    assert get_value(some_dictionary, 'outer_1') == 1
    assert get_value(some_dictionary, 'outer_3.level_2') == 3
    assert get_value(some_dictionary, ['outer_3', 'level_2']) == 3


def test_get_value_error(some_dictionary):
    with pytest.raises(expected_exception=Exception):
        get_value(some_dictionary, {'foo': 'bar'})


def test_set_value_1(some_dictionary):
    set_value(some_dictionary, ['outer_3', 'level_2'], 'hi')
    assert some_dictionary['outer_3']['level_2'] == 'hi'


def test_set_value_2(some_dictionary):
    set_value(some_dictionary, ['outer_4', ListIndex(1)], 'hi')
    assert some_dictionary['outer_4'][1] == 'hi'


def test_set_value_error(some_dictionary):
    with pytest.raises(expected_exception=Exception):
        set_value(some_dictionary, ['outer_4', 'doesnotexist'], 'noway')


def test_replace_by_path_value(dict_with_dup_keys):
    replace_by_path(dict_with_dup_keys, ['level_2'], 'replaced_value')
    assert dict_with_dup_keys['outer_2']['level_2'] == 'replaced_value'
    assert dict_with_dup_keys['outer_3']['level_2'] == 'replaced_value'


def test_replace_by_path_function_1(dict_with_dup_keys):
    def add_one(thing):
        return thing + 1

    replace_by_path(dict_with_dup_keys, ['level_2'], function=add_one)
    assert dict_with_dup_keys['outer_2']['level_2'] == 5
    assert dict_with_dup_keys['outer_3']['level_2'] == 4


def test_replace_by_path_value_in_list(dict_with_dup_keys):
    replace_by_path(dict_with_dup_keys, ['inner_1'], 'replaced_value')
    assert dict_with_dup_keys['outer_5'][0]['inner_1'] == 'replaced_value'
    assert dict_with_dup_keys['outer_5'][1]['inner_1'] == 'replaced_value'


def test_aggregate_values(dict_with_dup_keys):
    out = aggregate_values(dict_with_dup_keys, ['inner_1'])
    assert out == ['value_1', 'value_2']

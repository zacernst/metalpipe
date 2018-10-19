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


os.environ['PYTHONPATH'] = '.'
logging.basicConfig(level=logging.INFO)


def test_test_sanity():
    assert 1 == 1


@pytest.fixture(scope='function')  # By function because we terminate it
def simple_timed_dict():
    '''
    Fixture. Returns a `ConstantEmitter` that feeds into a `PrinterOfThings`
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

import os
import logging
import time

import pytest
from timed_dict.timed_dict import TimedDict
import nanostream.node as node
import nanostream.node_queue.queue as queue
import nanostream.message.poison_pill as poison_pill

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

def test_can_instantiate_timed_dict(simple_timed_dict):
    simple_timed_dict.stop_sweep()



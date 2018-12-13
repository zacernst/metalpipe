import os
import logging
import time

import pytest
import nanostream.node as node
import nanostream.node_queue.queue as queue
import nanostream.message.poison_pill as poison_pill

os.environ['PYTHONPATH'] = '.'
logging.basicConfig(level=logging.INFO)


def test_test_sanity():
    assert 1 == 1

def test_instantiate_node():
    obj = node.PrinterOfThings()
    assert isinstance(obj, node.PrinterOfThings)

def test_has_input_queue():
    obj = node.PrinterOfThings()
    assert isinstance(obj.input_queue_list, (list,))

@pytest.fixture(scope='function')  # By function because we terminate it
def simple_graph():
    '''
    Fixture. Returns a `ConstantEmitter` that feeds into a `PrinterOfThings`
    '''
    printer = node.PrinterOfThings()
    emitter = node.ConstantEmitter()
    printer.name = 'printer'
    emitter.name = 'emitter'
    emitter > printer
    return emitter

def test_linked_node_has_output_node_list(simple_graph):
    assert hasattr(simple_graph, 'output_node_list')

def test_linked_node_output_list_has_target(simple_graph):
    assert len(simple_graph.output_node_list) == 1

def test_linked_node_has_output_queue(simple_graph):
    assert (
        len(simple_graph.output_queue_list) == 1 and
        isinstance(
            simple_graph.output_queue_list[0],
            (queue.NanoStreamQueue,)))

def test_linked_node_shares_output_queue(simple_graph):
    connected_node = simple_graph.output_node_list[0]
    connected_node.input_queue_list[0] is simple_graph.output_queue_list[0]

def test_get_connected_nodes_from_simple_graph(simple_graph):
    connected_nodes = simple_graph.all_connected()
    assert len(connected_nodes) == 2

def test_start_simple_graph(simple_graph):
    simple_graph.global_start()
    time.sleep(2)
    assert simple_graph.thread_dict['printer'].is_alive()
    simple_graph.terminate_pipeline()

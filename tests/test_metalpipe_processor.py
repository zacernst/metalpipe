import os
import logging
import time

import pytest
import metalpipe.node as node
import metalpipe.node_queue.queue as queue

os.environ["PYTHONPATH"] = "."


def test_test_sanity():
    assert 1 == 1


def test_instantiate_node():
    obj = node.PrinterOfThings()
    assert isinstance(obj, node.PrinterOfThings)


def test_has_input_queue():
    obj = node.PrinterOfThings()
    assert isinstance(obj.input_queue_list, (list,))


@pytest.fixture(scope="function")
def constant_emitter():
    emitter = node.ConstantEmitter(thing={"foo": "bar"}, output_keypath="output")
    return emitter


@pytest.fixture(scope="function")
def printer_of_things():
    return node.PrinterOfThings(name="printer")


@pytest.fixture(scope="function")  # By function because we terminate it
def simple_graph(constant_emitter, printer_of_things):
    """
    Fixture. Returns a `ConstantEmitter` that feeds into a `PrinterOfThings`
    """
    constant_emitter > printer_of_things
    return constant_emitter


def test_linked_node_has_output_node_list(simple_graph):
    assert hasattr(simple_graph, "output_node_list")


def test_linked_node_output_list_has_target(simple_graph):
    assert len(simple_graph.output_node_list) == 1


def test_linked_node_has_output_queue(simple_graph):
    assert len(simple_graph.output_queue_list) == 1 and isinstance(
        simple_graph.output_queue_list[0], (queue.MetalPipeQueue,)
    )


def test_linked_node_shares_output_queue(simple_graph):
    connected_node = simple_graph.output_node_list[0]
    connected_node.input_queue_list[0] is simple_graph.output_queue_list[0]


def test_get_connected_nodes_from_simple_graph(simple_graph):
    connected_nodes = simple_graph.all_connected()
    assert len(connected_nodes) == 2


def test_start_simple_graph(simple_graph):
    simple_graph.global_start()
    time.sleep(2)
    assert simple_graph.thread_dict["printer"].is_alive()
    simple_graph.terminate_pipeline()


@pytest.fixture(scope="function")
def exception_raiser():
    class ExceptionRaiser(node.MetalNode):
        def process_item(self):
            assert False

    return ExceptionRaiser()


def test_error_throws_finished_flag(exception_raiser):
    emitter = node.ConstantEmitter(thing={"foo": "bar"}, output_keypath="output")
    emitter > exception_raiser
    emitter.global_start()
    emitter.wait_for_pipeline_finish()
    assert emitter.finished


def test_error_kills_threads(exception_raiser, constant_emitter):
    constant_emitter > exception_raiser
    constant_emitter.global_start()
    time.sleep(15)
    assert all(not i.is_alive() for i in constant_emitter.thread_dict.values())

import os
import logging
import time

import pytest
import metalpipe.node as node
import metalpipe.node_queue.queue as queue
import metalpipe.node_classes.network_nodes as network_nodes


os.environ["PYTHONPATH"] = "."
SAMPLE_DATA_DIR = './tests/sample_data/'
TRAVIS = os.environ.get('TRAVIS', False)


def test_test_sanity():
    assert 1 == 1


def test_http_get_request():
    if TRAVIS:
        assert True
    else:
        emitter = node.ConstantEmitter(
            thing={'user_number': '1'},
            output_keypath="output",
            max_loops=1)
        get_request_node = network_nodes.HttpGetRequest(
            endpoint_template='http://localhost:3000/users/1',
            endpoint_dict={},
            protocol='http',
            json=True)
        mock_node = node.MockNode()
        emitter > get_request_node > mock_node
        emitter.global_start()
        emitter.wait_for_pipeline_finish()
        assert mock_node.message_holder is not None
        assert isinstance(mock_node.message_holder, (dict,))
        assert 'id' in mock_node.message_holder
        assert mock_node.message_holder['id'] == 1


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
            yield {'exception_raiser': True}
            raise Exception()

    return ExceptionRaiser()


def test_error_throws_finished_flag(exception_raiser):
    emitter = node.ConstantEmitter(thing={"foo": "bar"}, output_keypath="output")
    emitter > exception_raiser
    emitter.global_start()
    emitter.wait_for_pipeline_finish()
    assert emitter.finished


def test_error_kills_threads(exception_raiser, constant_emitter):
    pr = node.PrinterOfThings()
    constant_emitter > exception_raiser > pr
    constant_emitter.global_start()
    constant_emitter.wait_for_pipeline_finish()
    time.sleep(10)
    assert all(not i.is_alive() for i in constant_emitter.thread_dict.values())


@pytest.fixture(scope="function")
def local_file_reader():
    file_reader = node.LocalFileReader(
        directory=SAMPLE_DATA_DIR,
        filename='customers.csv',
        output_key="contents"
    )
    return file_reader


def test_local_file_reader(local_file_reader):
    mock_node = node.MockNode()
    local_file_reader > mock_node
    local_file_reader.global_start()
    local_file_reader.wait_for_pipeline_finish()
    assert mock_node.message_holder is not None

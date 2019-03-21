import os
import logging
import time
import tempfile

import pytest
import metalpipe.metalpipe_recorder as metalpipe_recorder
import metalpipe.node as node


os.environ["PYTHONPATH"] = "."

CONSTANTS_IN_LOOP = 100


@pytest.fixture(scope="function")
def constant_emitter():
    emitter = node.ConstantEmitter(
        name="constant_emitter",
        thing={"foo": "bar"},
        output_keypath="output",
        max_loops=CONSTANTS_IN_LOOP,
        delay=0.05,
    )
    return emitter


@pytest.fixture(scope="function")
def printer_of_things():
    return node.PrinterOfThings(name="printer")


@pytest.fixture(scope="function")
def temporary_file():
    f = tempfile.NamedTemporaryFile()
    return f


@pytest.fixture(scope="function")  # By function because we terminate it
def simple_graph(constant_emitter, printer_of_things):
    """
    Fixture. Returns a `ConstantEmitter` that feeds into a `PrinterOfThings`
    """
    constant_emitter > printer_of_things
    return constant_emitter


def test_test_sanity():
    assert 1 == 1


@pytest.fixture(scope="function")
def metalpipe_recorder_with_graph(simple_graph, temporary_file):
    recorder = metalpipe_recorder.MetalPipeRecorder(
        pipeline=simple_graph, record_to_file=temporary_file.name
    )
    return recorder


def test_instantiate_metalpipe_recorder(metalpipe_recorder_with_graph):
    assert isinstance(
        metalpipe_recorder_with_graph, (metalpipe_recorder.MetalPipeRecorder,)
    )


def test_record_generator_output(simple_graph, temporary_file):
    simple_graph.global_start(record_to_file=temporary_file.name)
    time.sleep(10)
    values_stored = simple_graph.recorder.num_values_stored(simple_graph)
    print('values stored: ' + str(values_stored))
    assert values_stored == CONSTANTS_IN_LOOP

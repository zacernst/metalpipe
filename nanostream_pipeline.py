"""
Copyright (C) 2016 Zachary Ernst
zac.ernst@gmail.com

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import networkx as nx
import threading
import time
import multiprocessing as mp
from nanostream_processor import (
    NanoStreamProcessor, NanoStreamListener,
    NanoStreamSender,
    NanoStreamQueue)
import inspect


DEFAULT_MAX_QUEUE_SIZE = 128


class NanoStreamGraph(object):
    """
    They're actually directed graphs.
    """
    def __init__(self):
        self.graph = nx.DiGraph()
        self.node_list = []  # nodes are listeners, processors, etc.
        self.edge_list = []  # edges are queues
        self.thread_list = []  # We'll add these when `start` is calledt
        self.workers = []  # A list of functions to execute intermittantly
        self.worker_interval = None
        self.queue_constructor = NanoStreamQueue
        self.thread_constructor = threading.Thread  # For future mp support
        self.global_dict = {}  # For sharing and storing output from steps
        self.node_dict = {}

    def add_node(self, node):
        self.node_list.append(node)
        self.graph.add_node(node)
        node.global_dict = self.global_dict
        node.parent = self
        node_name, node_obj = [
            (i, j,) for i, j in
            inspect.getouterframes(inspect.currentframe())[
                -1].frame.f_globals.items() if j is node][0]
        if node_name not in self.node_dict:
            self.node_dict[node_name] = node_obj
        else:
            logging.warning('same name used for two nodes.')

    def __getattribute__(self, attr):
        '''
        Allow us to access `NanoStreamProcessor` nodes as attributes.
        '''
        if attr in super(
                NanoStreamGraph, self).__getattribute__('node_dict'):
            return super(
                NanoStreamGraph, self).\
                __getattribute__('node_dict')[attr]
        else:
            return super(NanoStreamGraph, self).__getattribute__(attr)

    def __add__(self, other):
        self.add_node(other)
        return self

    def __gt__(self, other):
        self.add_edge(self, other)


    def add_edge(self, source, target, **kwargs):
        """
        Create an edge connecting `source` to `target`. The edge
        is really just a queue
        """
        # No rails here --> no check for number of sources and sinks
        if isinstance(source, NanoStreamGraph):
            source = source.sinks[0]
        if isinstance(target, NanoStreamGraph):
            target = target.sources[0]
        max_queue_size = kwargs.get(
            'max_queue_size', DEFAULT_MAX_QUEUE_SIZE)
        edge_queue = self.queue_constructor(max_queue_size)
        # Following is for NetworkX, cuz why not?
        self.graph.add_edge(
            source, target)
        target.input_queue_list.append(edge_queue)
        source.output_queue_list.append(edge_queue)

    def add_worker(self, worker_object, interval=3):
        self.workers.append((worker_object, interval,))
        worker_object.parent = self

    @property
    def sources(self):
        return [node for node in self.node_list if node.is_source]

    @property
    def sinks(self):
        return [node for node in self.node_list if node.is_sink]

    @property
    def number_of_sources(self):
        return len(self.sources)

    @property
    def number_of_sinks(self):
        return len(number_of_sinks)

    def start(self, block=False):
        """
        We check whether any of the nodes have a "run_on_start" function.
        """
        for node in self.graph.nodes():
            if hasattr(node, 'run_on_start'):
                node.run_on_start()
        for node in self.graph.nodes():
            worker = self.thread_constructor(target=node.start)
            self.thread_list.append(worker)
            worker.start()
        for worker_tuple in self.workers:
            if not isinstance(worker_tuple[0], NanoGraphWorker):
                raise Exception("Needs to be a NanoGraphWorker")
            worker_tuple[0].graph = self

            def _thread_worker(self):
                while 1:
                    time.sleep(worker_tuple[1])
                    worker_tuple[0].worker()

            thread_worker = threading.Thread(
                target=_thread_worker, args=(self,))
            thread_worker.setDaemon(True)
            thread_worker.start()
            if block:
                thread_worker.join()


class NanoGraphWorker(object):
    """
    Subclass this, and override the `worker` method. Call `add_worker`
    on the `NanoStreamGraph` object.
    """
    def __init__(self):
        pass

    def worker(self, *args, **kwargs):
        raise NotImplementedError("Need to override worker method")


class NanoPrinter(NanoStreamProcessor):
    def process_item(self, message):
        print(message)


if __name__ == '__main__':
    pass

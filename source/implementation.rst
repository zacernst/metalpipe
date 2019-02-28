==============
Implementation
==============

This section describes what's happening under the hood in a ``MetalPipe``
data pipeline. Most people won't need to read this section.

The data journey
----------------

MetalPipe pipelines are sets of ``MetalNode`` objects connected by ``MetalPipeQueue``
objects. Think of each ``MetalNode`` as a vertex in a directed graph, and each
``MetalPipeQueue`` as a directed edge.

There are two types of ``MetalNode`` objects. A "source" is a ``MetalNode`` that does not accept incoming data from another ``MetalNode``. A "processor" is any ``MetalNode`` that is not a "source". Note that there is nothing in the class definition or object that distinguishes between these two -- the only
difference is that processors have a ``process_item`` method, and sources have a ``generator`` method. Other than that, they are identical.

The data journey begins with one or more source nodes. When a source node is started (by calling its ``start`` method), a new thread is created and the node's ``generator`` method is executed inside the thread. As results from the ``generator`` method are yielded, they are placed on each outgoing ``MetalPipeQueue`` to be picked up by one or more processors downstream.

The data from the source's ``generator`` is handled by the ``MetalPipeQueue`` object. At its heart, the ``MetalPipeQueue`` is simply a class which has a Python ``Queue.queue`` object as an attribute. The reason we don't simply use Python ``Queue`` objects is because the ``MetalPipeQueue`` contains some logic that's useful. In particular:

#. It wraps the data into a ``MetalPipeMessage`` object, which also holds useful metadata including a UUID, the ID of the node that generated the data, and a timestamp.
#. If the ``MetalPipeQueue`` receives data that is simply a ``None`` object, then it is skipped.





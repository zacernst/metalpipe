==============
Implementation
==============

This section describes what's happening under the hood in a ``NanoStream``
data pipeline. Most people won't need to read this section.

The data journey
----------------

NanoStream pipelines are sets of ``NanoNode`` objects connected by ``NanoStreamQueue``
objects. Think of each ``NanoNode`` as a vertex in a directed graph, and each
``NanoStreamQueue`` as a directed edge.

There are two types of ``NanoNode`` objects. A "source" is a ``NanoNode`` that does not accept incoming data from another ``NanoNode``. A "processor" is any ``NanoNode`` that is not a "source". Note that there is nothing in the class definition or object that distinguishes between these two -- the only
difference is that processors have a ``process_item`` method, and sources have a ``generator`` method. Other than that, they are identical.

The data journey begins with one or more source nodes. When a source node is started (by calling its ``start`` method), a new thread is created and the node's ``generator`` method is executed inside the thread. As results from the ``generator`` method are yielded, they are placed on each outgoing ``NanoStreamQueue`` to be picked up by one or more processors downstream.

The data from the source's ``generator`` is handled by the ``NanoStreamQueue`` object. At its heart, the ``NanoStreamQueue`` is simply a class which has a Python ``Queue.queue`` object as an attribute. The reason we don't simply use Python ``Queue`` objects is because the ``NanoStreamQueue`` contains some logic that's useful. In particular:

#. It wraps the data into a ``NanoStreamMessage`` object, which also holds useful metadata including a UUID, the ID of the node that generated the data, and a timestamp.
#. If the ``NanoStreamQueue`` receives data that is simply a ``None`` object, then it is skipped.





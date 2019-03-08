=======================
Node and Data Lifecycle
=======================

This section describes what's happening under the hood in a ``MetalPipe``
data pipeline. Most people won't need to read this section. But if you're
planning on writing custom classes that inherit from ``MetalNode``, this
will be helpful.

The Node Lifecycle
------------------

The ``MetalNode`` class is where the crucial work happens in a pipeline. The
lifecycle of a ``MetalNode`` object comprises several steps.

Instantiating the node and pipeline
===================================

Recall that when a node is defined in a configuration file, the definition
looks like this:

::

  my_node:
    class: MyMetalNodeClass
    options:
      an_option: foo
      another_option: bar

The code for any ``MetalNode`` subclass has an ``__init__`` method that has
the following form:

::

  class MyMetalNodeClass(MetalNode):
      def __init__(self, an_option=None, another_option=None, **kwargs):
          self.an_option = an_option
          self.another_option = another_option
          super(MyMetalNodeClass, self).__init__(**kwargs)

As you can see, the keyword arguments directly correspond to the keys under
the ``options`` key in the configuration file. When the configuration file is
read by the command-line tool, the class is instantiated and the options
are converted to keyword arguments to be passed to the constructor. Keyword
arguments will typically be a combination of options that are specific to
that class and options that are inherited by any subclass of ``MetalNode``.

Instantiating the class does not create any input or output queues. That
happens only when two nodes are hooked together. In python code, you can
hook up two or more nodes by using the ``>`` operator, as in:

::

  node_1 > node_2 > node_3

In a configuration file, this is accomplished with the ``paths`` key, like so:

::

  paths:
    -
      - node_1
      - node_2
      - node_3


Starting the node
=================

To do.

Processing data in the pipeline
===============================

To do.

Shutting down normally
======================

To do.

Shutting down due to error
==========================

To do.

The data journey
----------------

REVISE THIS

MetalPipe pipelines are sets of ``MetalNode`` objects connected by ``MetalPipeQueue``
objects. Think of each ``MetalNode`` as a vertex in a directed graph, and each
``MetalPipeQueue`` as a directed edge.

There are two types of ``MetalNode`` objects. A "source" is a ``MetalNode`` that does not accept incoming data from another ``MetalNode``. A "processor" is any ``MetalNode`` that is not a "source". Note that there is nothing in the class definition or object that distinguishes between these two -- the only
difference is that processors have a ``process_item`` method, and sources have a ``generator`` method. Other than that, they are identical.

The data journey begins with one or more source nodes. When a source node is started (by calling its ``start`` method), a new thread is created and the node's ``generator`` method is executed inside the thread. As results from the ``generator`` method are yielded, they are placed on each outgoing ``MetalPipeQueue`` to be picked up by one or more processors downstream.

The data from the source's ``generator`` is handled by the ``MetalPipeQueue`` object. At its heart, the ``MetalPipeQueue`` is simply a class which has a Python ``Queue.queue`` object as an attribute. The reason we don't simply use Python ``Queue`` objects is because the ``MetalPipeQueue`` contains some logic that's useful. In particular:

#. It wraps the data into a ``MetalPipeMessage`` object, which also holds useful metadata including a UUID, the ID of the node that generated the data, and a timestamp.
#. If the ``MetalPipeQueue`` receives data that is simply a ``None`` object, then it is skipped.

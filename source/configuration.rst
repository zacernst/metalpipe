Configuration file structure
============================

A configuration file starts with two top-level options, ``pipeline_name`` and ``pipeline_description``. These are optional, and are only used for the user's convenience.

Below those are two sections: ``nodes`` and ``paths``. Each ``nodes`` section contains one or more blocks that always have this form:

.. code-block:: none

    do_something:
      class: node class
      summary: optional string describing what this node does
      options:
        option_1: value of this option
        option_2: value of another option


Let's go through this one line at a time.

Each node block describes a single node in the MetalPipe pipeline. A node
must be given a name, which can be any arbitrary string. This should be a
short, descriptive string describing its action, such as ``get_environment_variables`` or ``parse_json``, for example. We encourage
you to stick to a clear naming convention. We like nodes to have names of
the form ``verb_noun`` (as in ``print_name``).

MetalPipe contains a number of node classes, each of which is designed
for a specific type of ETL task. In the sample configuration, we've used
the built-in classes ``GetEnvironmentVariables`` and ``PrinterOfThings``; these are the value following ``class``. You can also roll your own node classes (we'll describe how to do this later in the documentation).

Next is a set of keys and values for the various options that are supported by that class. Because each node class does something different,
the options are different as well. In the sample configuration, the
``GetEnvironmentVariables`` node class requires a list of environment variables to retrieve, so as you would expect, we specify that list under the ``environment_variables`` option. The various options are explained in
the documentation for each class. In addition to the options that are specific to each node, there are also options that are common to every type of node. These will be explained later.

The structure of the pipeline is given in the ``paths`` section, which contains a list of lists. Each list is a set of nodes that are to be linked together in
order. In our example, the ``paths`` value says that
``get_environment_variables`` will send its output to ``print_variables``.
Paths can be arbitrarily long.

If you wanted to send the environment variables down two different execution
paths, you add another list to the ``paths``, like so:

.. code-block:: none

    paths:
      - 
        - get_environment_variables
        - print_variables
      -
        - get_environment_variables
        - do_something_else
        - and_then_do_this


With this set of ``paths``, the pipeline looks like a very simple tree, with
``get_environment_variables`` at the root, which branches to
``print_variables`` and ``do_something_else``.

When you have written the configuration file, you're ready to use the
MetalPipe CLI. It accepts a command, followed by some options. As of now, the
commands it accepts are ``run``, which executes the pipeline, and ``draw``,
which generates a diagram of the pipeline. The relevant command(s) are:

.. code-block:: none

    python metalpipe_cli.py [run | draw] --filename my_sample_config.yaml


The ``metalpipe`` command can generate a pdf file containing a drawing of the pipeline, showing the flow of data through the various nodes. Just speciy ``draw`` instead of ``run`` to generate the diagram. For our simple little pipeline, we get this:

.. figure:: sample_config_drawing.png
  :width: 240
  :alt: Sample pipeline drawing

  The pipeline drawing for the simple configuration example

It is also possible to skip using the configuration file and define your
pipelines directly in code. In general, it's better to use the configuration
file for a variety of reasons, but you always have the option of doing this
in Python.

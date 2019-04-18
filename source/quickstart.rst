Quickstart
==========

This explains how to install MetalPipe, create a simple configuration file, and execute a pipeline.


Install MetalPipe
-----------------

MetalPipe is installed in the usual way, with pip:

::

    pip install metalpipe

To test your installation, try typing

::

    metalpipe --help

If MetalPipe is installed correctly, you should see a help message.


On a Debian-like Linux system, you may have to do:

::

   sudo apt-get install python3-dev default-libmysqlclinet-dev gcc 

or whatever is the equivalent for your distribution.


Write a configuration file
--------------------------

You use MetalPipe by (1) writing a configuration file that describes your pipeline, and (2) running the ``metalpipe`` command, specifying the location of your
configuration file. MetalPipe will read the configuration, create the pipeline,
and run it.

The configuration file is written in YAML. It has three parts:

1. A list of global variables (optional)
#. The nodes and their options (required)
#. A list of edges connecting those nodes to each other.

This is a simple configuration file. If you want to, you can copy it into a
file called ``sample_config.yaml``:

.. code-block:: none

    ---
    pipeline_name: Sample MetalPipe configuration
    pipeline_description: Reads some environment variables and prints them

    nodes:
      get_environment_variables:
        class: GetEnvironmentVariables
        summary: Gets all the necessary environment variables
        options:
          environment_variables:
            - API_KEY
            - API_USER_ID

      print_variables:
        class: PrinterOfThings
        summary: Prints the environment variables to the terminal
        options:
          prepend: "Environment variables: "

    paths:
      - 
        - get_environment_variables
        - print_variables


Run the pipeline
----------------

If you've installed MetalPipe and copied this configuration into ``sample_config.yaml``, then you can execute the pipeline:

.. code-block:: none

    metalpipe run --filename sample_config.yaml


The output should look like this (you might also see some log messages):

.. code-block:: none
    
    Environment variables: 
    {'API_USER_ID': None, 'API_KEY': None}


The MetalPipe pipeline has found the values of two environment variables (``API_KEY`` and ``API_USER_ID``) and printed them to the terminal. If those environmet variables have not been set, their values will be ``None``. But if you were to set any of them, their values would be printed.

Testing
-------

In order to run the various unit tests, you'll need to have access to some fixturized REST API data. We
use ``json-server`` for this purpose. You'll need to install it:

.. code-block:: none

    npm install -g json-server 

The ``run_tests.sh`` script will start the JSON server and run all the tests. When it completes, the JSON server will be killed. Tests without the JSON server can be run by executing ``run_travis_tests.sh`` instead. This is the script that's used for CI.

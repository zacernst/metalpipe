[![Build Status](https://travis-ci.org/zacernst/nanostream.svg?branch=master)](https://travis-ci.org/zacernst/nanostream)

# nanostream: Small-scale streaming data

## What is it? Why is it?

**Hadoop is to Redis as Spark is to NanoStream**

We love stream processing. It's a great model for lots of work, especially ETL.
There are excellent stream processing tools such as Spark, Flink, and Storm. They're designed for handling huge amounts of Big Data(tm). But they carry a lot of overhead because they're Big Data(tm) tools.

Most of our data problems are not Big Data(tm). They're also not Small Data. They're Medium Data -- that is, data that's big enough to require some planning, but not so big as to justify the infrastructure and complexity overhead that come with Spark and its cousins.

NanoStream lets you deploy a streaming application with no overhead. It's 
entirely self-contained, and runs on a single core (which, let's face is,
is more than enough processing power for 99% of your work). NanoStream sets up 
each step in your pipeline in its own thread, so there are no bottlenecks. It 
monitors all the threads and queues, and logs any problems. If the data comes 
in faster than NanoStream can handle, it applies back-pressure to the data 
stream. But in reality, because NanoStream doesn't have any of the overhead 
of distributed systems, it's pretty fast.

# Using NanoStream

You use NanoStream by specifying one or more `NanoNode` objects, linking them
together into a pipeline (an acyclic directed graph), and starting them. Several
`NanoNode` classes are provided, and it's easy to create new ones. Here are some
types of examples:

## Using built-in `NanoNode` classes

Let's say you want to watch a directory for new CSV files, read them when
they appear, iterate over all the rows, and print those rows as they arrive.
You can do so by importing a few classes, instantiating them, and running them
in a pipeline like so:

```
    # Instantiate the classes:
    watchdog = LocalDirectoryWatchdog(directory='./data_directory')
    file_reader = LocalFileReader(serialize=False)
    csv_reader = CSVReader()
    printer = PrinterOfThings()
    # Use ">" to create connections between the nodes
    watchdog > file_reader > csv_reader > printer
    # Start it
    watchdog.global_start()
```

The result will be a streaming pipeline that monitors `data_directory/`,
printing the rows of any CSV file that appears there (or is modified).

## Rolling your own `NanoNode` class



# This is an alpha release

'nuff said.

zac.ernst@gmail.com

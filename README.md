[![Build Status](https://travis-ci.org/zacernst/metalpipe.svg?branch=master)](https://travis-ci.org/zacernst/nanostream)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Readthedocs](https://readthedocs.org/projects/metalpipe/badge/)](http://metalpipe.reathedocs.io)


# MetalPipe: Modules for ETL Pipelines

MetalPipe is a lightweight, multithreaded framework for building ETL pipelines. It utilizes a design pattern similar to stream-processing frameworks such as Spark or Storm. But unlike those heavyweight systems, MetalPipe is designed for ETL, not data analytics.

The goals of MetalPipe are:

1. To speed up ETL pipeline development by replacing as much code as possible with simple configurations.
2. To make ETL pipelines faster by eliminating IO bottlenecks.
3. To enable robust monitoring and error-handling into all ETL pipelines by default.
4. To eliminate the need for specialized, heavyweight infrastructure for ETL jobs.

Documentation lives here:

https://metalpipe.readthedocs.io/en/latest/

MetalPipe is a work in progress. Although we use it in production, it should not generally be considered stable.

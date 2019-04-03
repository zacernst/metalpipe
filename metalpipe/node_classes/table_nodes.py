"""
Table Nodes Module
==================

Analyzing tables, converting types, etc. Not for interacting directly with
tabular databases
"""

import requests
import collections
import json
import time
import random
import logging
from metalpipe.node import MetalNode, NothingToSeeHere
from bloom_filter import BloomFilter
from metalpipe.utils.data_structures import make_types


class IntermediateDataType:
    def __init__(self):
        pass


class VarChar(IntermediateDataType):
    pass


PYTHON_PRIMITIVE_DATA_TYPES = [int, float, str, bytes]


def get_type_information(thing):
    thing = str(thing)
    possible_types = []
    for data_type in PYTHON_PRIMITIVE_DATA_TYPES:
        try:
            data_type(thing)
            possible_types.append(data_type)
        except:
            pass
    return None


class RowStatCollector(MetalNode):
    def __init__(self, **kwargs):
        self.bloom_filter_dict = collections.defaultdict(BloomFilter)
        self.uniqueness_dict = collections.defaultdict(bool)
        self.num_unique_values_dict = collections.defaultdict(int)
        self.null_dict = collections.defaultdict(bool)
        self.first_row = True
        super(RowStatCollector, self).__init__(**kwargs)

    def process_item(self):
        self.log_info("rowstat: " + str(self.__message__))
        if isinstance(self.__message__, (dict,)):
            rows = [self.__message__]
        elif isinstance(self.__message__, (list, tuple, set)):
            rows = self.__message__
        else:
            raise Exception("This should not happen.")
        self.log_info("Stats has a row...")
        for row in rows:
            if self.first_row:
                for column, _ in row.items():
                    self.uniqueness_dict[
                        column
                    ] = True  # Default unique to True
                    self.null_dict[column] = False  # NULL to False
                self.first_row = False
            for column, value in row.items():
                # Use bloom filter to check for unique values
                if value in self.bloom_filter_dict[column]:
                    self.uniqueness_dict[column] = False
                else:
                    self.num_unique_values_dict[column] += 1
                    self.bloom_filter_dict[column].add(value)

                # Record whether a NULL occurs in the column
                if value is None:
                    self.null_dict[column] = True
        yield NothingToSeeHere()

    def cleanup(self):
        yield self.num_unique_values_dict


if __name__ == "__main__":
    SAMPLE_DATA_FILE = "~/github/metalpipe/sample_data/customers.csv"
    from metalpipe.node import (
        CSVReader,
        LocalFileReader,
        ConstantEmitter,
        PrinterOfThings,
    )

    file_name_emitter = ConstantEmitter(
        name="filename",
        thing="customers.csv",
        max_loops=1,
        output_key="file_name",
    )
    file_reader = LocalFileReader(
        name="file_reader",
        directory="./sample_data",
        key="file_name",
        output_key="contents",
    )
    csv_reader = CSVReader(name="csv_reader", key="contents", output_key="row")
    printer = PrinterOfThings(
        pretty=True, disable=False, prepend=">>>>>", name="printer"
    )
    row_stats = RowStatCollector(key="row", name="stats", output_key="stats")
    file_name_emitter > file_reader > csv_reader > row_stats > printer
    # file_name_emitter > file_reader > csv_reader > printer
    file_name_emitter.global_start()
    # file_name_emitter.terminate_pipeline()

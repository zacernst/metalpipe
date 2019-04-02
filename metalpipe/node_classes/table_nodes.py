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


PYTHON_PRIMITIVE_DATA_TYPES = [
    int,
    float,
    str,
    bytes,]


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
        if isinstance(self.__message__, (dict,)):
            rows = [self.__message__]
        elif isinstance(self.__message__, (list, tuple, set,)):
            rows = self.__message__
        else:
            raise Exception('This should not happen.')
        for row in rows:
            if self.first_row:
                for column, _ in row.items():
                    self.uniqueness_dict[column] = True  # Default unique to True
            for column, value in row.items():
                # Use bloom filter to check for unique values
                if value in self.bloom_filter_dict[column]:
                    self.uniqueness_dict[column] = False
                else:
                    self.num_unique_values_dict[column] += 1
                    self.uniqueness_dict[column].add(value)

                # Record whether a NULL occurs in the column
                if value is None:
                    self.null_dict[column] = False
        yield NothingToSeeHere()

    def cleanup():
        pass


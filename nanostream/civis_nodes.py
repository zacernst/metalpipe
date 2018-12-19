"""
Civis-specific node types
=========================

This is where any classes specific to the Civis API live.
"""

import logging
import os
import tempfile
import csv
import civis

from nanostream.node import *
from nanostream.node_classes.network_nodes import HttpGetRequestPaginator
from nanostream.utils.helpers import remap_dictionary

logging.basicConfig(level=logging.ERROR)


class SendToCivis(NanoNode):
    def __init__(
        self,
        *args,
        civis_api_key=None,
        civis_api_key_env_var='CIVIS_API_KEY',
        database=None,
        schema=None,
        include_columns=None,
        block=False,
        table_name=None,
        remap=None,
            **kwargs):
        self.civis_api_key = civis_api_key or os.environ[civis_api_key_env_var]
        self.include_columns = include_columns
        self.table_name = table_name
        self.schema = schema
        self.database = database
        self.block = block
        self.remap = remap
        self.full_table_name = '.'.join([self.schema, self.table_name])

        if self.civis_api_key is None or len(self.civis_api_key) == 0:
            raise Exception('Could not get a Civis API key.')

        super(SendToCivis, self).__init__(**kwargs)

    def setup(self):
        '''
        Not sure if we'll need this. We could get a client and pass it around.
        '''

    def process_item(self):
        '''
        Accept a bunch of dictionaries mapping column names to values.
        '''
        with tempfile.NamedTemporaryFile(mode='w') as tmp:
            if self.include_columns is not None:
                fieldnames = self.include_columns
            elif self.remap is not None:
                fieldnames = list(self.remap.keys())
            else:
                fieldnames = self.message[0].keys()
            writer = csv.DictWriter(tmp, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in self.message:
                # Optionally remap row here
                if self.remap is not None:
                    row = remap_dictionary(row, self.remap)
                writer.writerow(row)
            tmp.flush()
            fut = civis.io.csv_to_civis(
                tmp.name, self.database, self.full_table_name, existing_table_rows='append')
            if self.block:
                result = fut.result()
        yield self.message


if __name__ == '__main__':
    pass

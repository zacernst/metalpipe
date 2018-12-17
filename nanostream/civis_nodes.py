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
            **kwargs):
        self.civis_api_key = civis_api_key or os.environ[civis_api_key_env_var]
        self.include_columns = include_columns
        self.table_name = table_name
        self.schema = schema
        self.database = database
        self.block = block
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
            fieldnames = (
                self.include_columns if self.include_columns is not None
                else self.message[0].keys())
            writer = csv.DictWriter(tmp, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in self.message:
            #    row = {bytes(key, 'utf8'): bytes(value, 'utf8') for key, value in row.items()}
                writer.writerow(row)
            tmp.flush()
            fut = civis.io.csv_to_civis(
                tmp.name, self.database, self.full_table_name, existing_table_rows='append')
            if self.block:
                result = fut.result()
        yield self.message


if __name__ == '__main__':

    import time

    ONE_DAY = 3600 * 24 * 1000
    NOW = int(float(time.time())) * 1000
    TWO_WEEKS_AGO = str(
        int(float(NOW - (14 * ONE_DAY))))

    HUBSPOT_TEMPLATE = (
        'https://api.hubapi.com/email/public/v1/'
        'events?hapikey={HUBSPOT_API_KEY}&'
        'startTimestamp={start_timestamp}&'
        'endTimestamp={end_timestamp}&'
        'limit=50&'
        'offset={offset}')

    endpoint_dict = {
        # 'hubspot_api_key': HUBSPOT_API_KEY,
        'start_timestamp': TWO_WEEKS_AGO,
        'end_timestamp': NOW}

    paginator = HttpGetRequestPaginator(
        endpoint_dict=endpoint_dict,
        pagination_get_request_key='offset',
        endpoint_template=HUBSPOT_TEMPLATE,
        additional_data_key='hasMore',
        pagination_key='offset',
        name='Hubspot paginator')

    env_vars = GetEnvironmentVariables(
        'HUBSPOT_API_KEY',
        'HUBSPOT_USER_ID',
        name='Environment variables')

    to_redshift = SendToCivis(
        database='Greenpeace',
        schema='staging',
        table_name='hubspot_test',
        block=False,
        input_message_keypath='thing',
        include_columns=['recipient', 'created'],
        name='To Redshift')

    remap_output = Remapper({'thing': ['events']}, name='Remap keys')

    printer = PrinterOfThings()

    env_vars > paginator > remap_output > to_redshift

    env_vars.global_start(pipeline_name='Hubspot ingestion', datadog=False)

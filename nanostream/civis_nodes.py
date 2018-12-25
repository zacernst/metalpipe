"""
Civis-specific node types
=========================

This is where any classes specific to the Civis API live.
"""

import logging
import os
import tempfile
import time
import csv
import sys
import uuid
import civis

from nanostream.node import *
from nanostream.node_classes.network_nodes import HttpGetRequestPaginator
from nanostream.utils.helpers import remap_dictionary
from timed_dict.timed_dict import TimedDict

MONITOR_FUTURES_SLEEP = 2

class SendToCivis(NanoNode):
    def __init__(
        self,
        *args,
        civis_api_key=None,
        civis_api_key_env_var='CIVIS_API_KEY',
        database=None,
        schema=None,
        existing_table_rows='append',
        include_columns=None,
        dummy_run=False,
        block=False,
        max_errors=0,
        table_name=None,
        columns=None,
        remap=None,
        recorded_tables=TimedDict(timeout=300),
            **kwargs):
        self.civis_api_key = civis_api_key or os.environ[civis_api_key_env_var]
        self.include_columns = include_columns
        self.table_name = table_name
        self.dummy_run = dummy_run
        self.schema = schema
        self.max_errors = int(max_errors)
        self.existing_table_rows = existing_table_rows
        self.database = database
        self.block = block
        self.remap = remap
        self.recorded_tables = recorded_tables
        self.full_table_name = '.'.join([self.schema, self.table_name])
        self.columns = columns

        if self.civis_api_key is None or len(self.civis_api_key) == 0:
            raise Exception('Could not get a Civis API key.')

        super(SendToCivis, self).__init__(**kwargs)
        self.monitor_futures_thread = threading.Thread(
            target=SendToCivis.monitor_futures, args=(self, ), daemon=True)
        self.monitor_futures_thread.start()

    def setup(self):
        '''
        Not sure if we'll need this. We could get a client and pass it around.
        '''

    def monitor_futures(self):
        run = True
        while run:
            for table_id, future_dict in list(self.recorded_tables.items()):
                future_obj = future_dict['future']
                row_list = future_dict['row_list']
                if future_obj.done():
                    if future_obj.failed():
                        logging.info(future_obj.exception())
                        self.status = 'error'
                        run = False
            time.sleep(MONITOR_FUTURES_SLEEP)

    def process_item(self):
        '''
        Accept a bunch of dictionaries mapping column names to values.
        '''

        # with tempfile.NamedTemporaryFile(mode='w') as tmp:
        row_list = []
        with open(uuid.uuid4().hex + self.full_table_name + '.csv', 'w') as tmp:
            if self.include_columns is not None:
                fieldnames = self.include_columns
            elif self.columns is not None:
                fieldnames = self.columns
            else:
                fieldnames = sorted(list(self.message[0].keys()))
            writer = csv.DictWriter(
                tmp,
                fieldnames=fieldnames,
                extrasaction='ignore',
                quoting=csv.QUOTE_ALL)
            writer.writeheader()
            row_list.append(fieldnames)
            for row in self.message:
                # Optionally remap row here
                if self.remap is not None:
                    row = remap_dictionary(row, self.remap)
                #if 'is_contact' in row:
                #    row['is_contact'] = 'foobar'
                writer.writerow(row)
                row_list.append(row)  # Will this get too slow?
            tmp.flush()
            if not self.dummy_run:
                fut = civis.io.csv_to_civis(
                    tmp.name,
                    self.database,
                    self.full_table_name,
                    max_errors=self.max_errors,
                    headers=True,
                    existing_table_rows=self.existing_table_rows)
                table_id = uuid.uuid4()
                self.recorded_tables[table_id.hex] = {
                    'row_list': row_list,
                    'future': fut}
                if self.block:
                    while not fut.done():
                        time.sleep(1)
            else:
                logging.info('Not sending to Redshift due to `dummy run`')
        yield self.message


class EnsureCivisRedshiftTableExists(NanoNode):

    def __init__(
        self,
        on_failure='exit',
        table_name=None,
        schema_name=None,
        columns=None,
        block=True,
            **kwargs):

        self.on_failure = on_failure
        self.table_name = table_name
        self.schema_name = schema_name
        self.columns = columns
        self.block = block
        if any(i is None for i in [
                on_failure, table_name, schema_name, columns]):
            raise Exception('Missing parameters.')
        super(EnsureCivisRedshiftTableExists, self).__init__(**kwargs)

    def process_item(self):
        yield self.message

    def generator(self):
        columns_spec = ', '.join(
            ['"{column_name}" {column_type} NULL'.format(
                column_name=column['column_name'],
                column_type=column['column_type']) for column in self.columns])
        create_statement = (
            '''CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" '''
            '''({columns_spec});'''.format(
                schema_name=self.schema_name,
                table_name=self.table_name,
                columns_spec=columns_spec))
        fut = civis.io.query_civis(create_statement, 'Greenpeace')
        result = fut.result()

        yield columns_spec


if __name__ == '__main__':
    # Test so that we can get a better view into errors
    api_key = os.environ['CIVIS_API_KEY']
    fut = civis.io.csv_to_civis(
        'email_test_data.csv',
        'Greenpeace',
        'staging.email_raw',
        max_errors=0,
        headers=True,
        existing_table_rows='append')

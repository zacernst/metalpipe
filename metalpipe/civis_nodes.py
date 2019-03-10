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
from civis.base import DONE

from metalpipe.node import *
from metalpipe.node_classes.network_nodes import HttpGetRequestPaginator
from metalpipe.utils.helpers import remap_dictionary, SafeMap, list_to_dict
from timed_dict.timed_dict import TimedDict

MONITOR_FUTURES_SLEEP = 5


class SendToCivis(MetalNode):
    def __init__(
        self,
        *args,
        civis_api_key=None,
        civis_api_key_env_var="CIVIS_API_KEY",
        database=None,
        schema=None,
        existing_table_rows="append",
        include_columns=None,
        dummy_run=False,
        block=False,
        max_errors=0,
        table=None,
        columns=None,
        remap=None,
        recorded_tables=TimedDict(timeout=30),
        **kwargs
    ):
        self.civis_api_key = civis_api_key or os.environ[civis_api_key_env_var]
        self.include_columns = include_columns
        self.table = table
        self.dummy_run = dummy_run
        self.schema = schema
        self.max_errors = int(max_errors)
        self.existing_table_rows = existing_table_rows
        self.database = database
        self.block = block
        self.remap = remap
        self.recorded_tables = recorded_tables
        self.full_table_name = ".".join([self.schema, self.table])
        self.columns = columns

        if self.civis_api_key is None and len(self.civis_api_key) == 0:
            raise Exception("Could not get a Civis API key.")

        super(SendToCivis, self).__init__(**kwargs)
        self.monitor_futures_thread = threading.Thread(
            target=SendToCivis.monitor_futures, args=(self,), daemon=True
        )
        self.monitor_futures_thread.start()

    def setup(self):
        """
        Not sure if we'll need this. We could get a client and pass it around.
        """
        pass

    def monitor_futures(self):
        class DummyResult:
            def __init__(self):
                self.state = "done"

        run = True
        while run:
            logging.debug("Checking future objects...")

            table_lock = threading.Lock()
            table_lock.acquire(blocking=True)

            try:
                table_items = list(self.recorded_tables.items())
            except RuntimeError:
                logging.warning(
                    "Runtime error in dictionary comprehension. Continuing."
                )
                continue

            for table_id, future_dict in table_items:
                future_obj = future_dict["future"]
                row_list = future_dict["row_list"]
                # logging.debug(future_obj.done())
                logging.debug(
                    "poller result:"
                    + str(future_obj._state)
                    + str(type(future_obj._state))
                )
                if future_obj._state != "RUNNING":
                    if future_obj.failed():
                        logging.debug(
                            "failer in SendToCivis: "
                            + str(future_obj.exception())
                        )
                        self.status = (
                            "error"
                        )  # Needs to be caught by Node class
                        run = False
            table_lock.release()
            time.sleep(MONITOR_FUTURES_SLEEP)
        for table_id, future_dict in list(self.recorded_tables.items()):
            setattr(future_dict["future"], "done", lambda: True)
            future_dict["future"].set_result(DummyResult())
            # future_dict['future'].cleanup()
            # CivisFuture object, not ``Future`` -- this is why the problems!

    def process_item(self):
        """
        Accept a bunch of dictionaries mapping column names to values.
        """

        # with tempfile.NamedTemporaryFile(mode='w') as tmp:
        row_list = []
        if self.name == "send_email_to_redshift":
            logging.debug("send_email_to_redshift")
            logging.debug(str(self.__message__))
        if len(self.__message__) == 0:
            yield self.message

        else:
            with tempfile.NamedTemporaryFile(mode="w") as tmp:
                if self.include_columns is not None:
                    fieldnames = self.include_columns
                elif self.columns is not None:
                    fieldnames = self.columns
                else:
                    try:
                        fieldnames = sorted(list(self.__message__[0].keys()))
                    except:
                        import pdb

                        pdb.set_trace()
                writer = csv.DictWriter(
                    tmp,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                    quoting=csv.QUOTE_ALL,
                )
                writer.writeheader()
                row_list.append(fieldnames)
                for row in self.__message__:
                    # Optionally remap row here
                    if self.remap is not None:
                        row = remap_dictionary(row, self.remap)
                    # if 'is_contact' in row:  # Boom
                    #    row['is_contact'] = 'barbar'
                    writer.writerow(row)
                    row_list.append(row)  # Will this get too slow?
                tmp.flush()
                logging.debug("to redshift: " + str(self.name))
                logging.debug(str(row_list))
                if not self.dummy_run:
                    fut = civis.io.csv_to_civis(
                        tmp.name,
                        self.database,
                        self.full_table_name,
                        max_errors=self.max_errors,
                        headers=True,
                        existing_table_rows=self.existing_table_rows,
                    )
                    table_id = uuid.uuid4()
                    self.recorded_tables[table_id.hex] = {
                        "row_list": row_list,
                        "future": fut,
                    }
                    if self.block:
                        while not fut.done():
                            time.sleep(1)
                else:
                    logging.info("Not sending to Redshift due to `dummy run`")
            yield self.message


class EnsureCivisRedshiftTableExists(MetalNode):
    def __init__(
        self,
        on_failure="exit",
        table=None,
        schema=None,
        columns=None,
        block=True,
        **kwargs
    ):

        self.on_failure = on_failure
        self.table = table
        self.schema = schema
        self.columns = columns
        self.block = block
        if any(i is None for i in [on_failure, table, schema, columns]):
            raise Exception("Missing parameters.")
        super(EnsureCivisRedshiftTableExists, self).__init__(**kwargs)

    def process_item(self):
        for i in self.generator():
            yield i

    def generator(self):
        columns_spec = ", ".join(
            [
                '"{column_name}" {column_type} NULL'.format(
                    column_name=column["column_name"],
                    column_type=column["column_type"],
                )
                for column in self.columns
            ]
        )
        create_statement = (
            """CREATE TABLE IF NOT EXISTS "{schema}"."{table}" """
            """({columns_spec});""".format(
                schema=self.schema, table=self.table, columns_spec=columns_spec
            )
        )
        logging.debug("Ensuring table exists -- " + create_statement)
        fut = civis.io.query_civis(create_statement, "Greenpeace")
        result = fut.result()

        yield columns_spec


class FindValueInRedshiftColumn(MetalNode):
    def __init__(
        self,
        on_failure="exit",
        table=None,
        database=None,
        schema=None,
        column=None,
        choice="max",
        **kwargs
    ):

        self.on_failure = on_failure
        self.table = table
        self.schema = schema
        self.database = database
        self.column = column
        self.database = database
        self.choice = choice.upper()

        if self.choice not in ["MAX", "MIN"]:
            raise Exception(
                "The `choice` parameter must be one of [MAX, MIN]."
            )
        super(FindValueInRedshiftColumn, self).__init__(**kwargs)

    def process_item(self):
        for i in self.generator():
            yield i

    def generator(self):
        create_statement = """SELECT {choice}({column}) FROM {schema}.{table};""".format(
            schema=self.schema,
            table=self.table,
            column=self.column,
            choice=self.choice.upper(),
        )
        fut = civis.io.query_civis(create_statement, self.database)
        result = fut.result()
        value = (
            result["result_rows"][0][0]
            if len(result["result_rows"]) > 0
            and len(result["result_rows"][0]) > 0
            else None
        )
        logging.debug("FindValueInRedshiftColumn: " + str(value))
        yield value


class CivisSQLExecute(MetalNode):
    """
    Execute a SQL statement and return the results.
    """

    def __init__(
        self,
        *args,
        sql=None,
        civis_api_key=None,
        civis_api_key_env_var="CIVIS_API_KEY",
        database=None,
        dummy_run=False,
        query_dict=None,
        returned_columns=None,
        **kwargs
    ):
        self.sql = sql
        self.query_dict = query_dict or {}
        self.civis_api_key = civis_api_key or os.environ[civis_api_key_env_var]
        self.dummy_run = dummy_run
        self.database = database
        self.returned_columns = returned_columns

        if self.civis_api_key is None and len(self.civis_api_key) == 0:
            raise Exception("Could not get a Civis API key.")

        super(CivisSQLExecute, self).__init__(**kwargs)

    def process_item(self):
        """
        Execute a SQL statement and return the result.
        """
        sql_query = self.sql.format_map(SafeMap(**self.query_dict))
        sql_query = sql_query.format_map(SafeMap(**(self.message or {})))
        logging.debug(sql_query)
        if not self.dummy_run:
            fut = civis.io.query_civis(sql_query, self.database)
            result = fut.result()
        else:
            logging.info("Not querying Redshift due to `dummy run`")
            result = None
        result_rows = result["result_rows"]
        if self.returned_columns is not None:
            result_rows = [
                list_to_dict(row, self.returned_columns) for row in result_rows
            ]
        else:
            result_rows = result["result_rows"]
        yield {"result_rows": result_rows}


class CivisToCSV(MetalNode):
    """
    Execute a SQL statement and return the results via a CSV file.
    """

    def __init__(
        self,
        *args,
        sql=None,
        civis_api_key=None,
        civis_api_key_env_var="CIVIS_API_KEY",
        database=None,
        dummy_run=False,
        query_dict=None,
        returned_columns=None,
        include_headers=True,
        delimiter=",",
        **kwargs
    ):
        self.sql = sql
        self.query_dict = query_dict or {}
        self.civis_api_key = civis_api_key or os.environ[civis_api_key_env_var]
        self.dummy_run = dummy_run
        self.database = database
        self.returned_columns = returned_columns
        self.include_headers = include_headers
        self.delimiter = delimiter

        if self.civis_api_key is None and len(self.civis_api_key) == 0:
            raise Exception("Could not get a Civis API key.")

        super(CivisToCSV, self).__init__(**kwargs)

    def process_item(self):
        """
        Execute a SQL statement and return the result.
        """
        sql_query = self.sql.format_map(SafeMap(**self.query_dict))
        sql_query = sql_query.format_map(SafeMap(**(self.message or {})))
        tmp_filename = uuid.uuid4().hex + "_tmp.csv"
        fut = civis.io.civis_to_csv(tmp_filename, sql_query, self.database)
        fut.result()
        # while fut._result == 'RUNNING':
        #    time.sleep(1)
        # logging.debug('future state: ' + str(fut._state))
        try:
            csv_file = open(tmp_filename, "r")
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                yield row
            os.remove(tmp_filename)
        except FileNotFoundError:
            logging.debug("FileNotFoundError in CivisToCSV")
            yield NothingToSeeHere()


if __name__ == "__main__":
    # Test so that we can get a better view into errors
    api_key = os.environ["CIVIS_API_KEY"]
    fut = civis.io.csv_to_civis(
        "email_test_data.csv",
        "Greenpeace",
        "staging.email_raw",
        max_errors=0,
        headers=True,
        existing_table_rows="append",
    )

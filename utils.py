# 12823

import concurrent.futures
import copy
import string
import argparse
import pickle
import pyaml
import json
from jsonschema import validate
import requests
import types
import pprint
import inspect
import logging
import importlib
import os
import collections
import io
import hashlib
import pandas as pd
import numpy as np
import civis

# CREATE TABLE hubdialer_civis_test.campaign_questions AS SELECT DISTINCT campaign_id, question FROM hubdialer_civis_test.denormalized_household_attempts ORDER BY campaign_id;

logging.basicConfig(level=logging.INFO)

# Default API for testing purposes only
API_GET_URL = 'https://jsonplaceholder.typicode.com/users/1'


# There is no specific type for `None`. Let's make one.
NoneType = type(None)


# Mapping from Python types to primitive JSON types
PRIMITIVE_TYPE_DICT = {
    str: 'string',
    int: 'number',
    float: 'number',
    NoneType: 'null',
    bool: 'boolean',}


# Mapping from Python structures to JSON structures
OTHER_TYPE_DICT = {
    list: 'array',
    dict: 'object',}


# Contains all type mappings
JSON_TYPE_DICT = {}
JSON_TYPE_DICT.update(PRIMITIVE_TYPE_DICT)
JSON_TYPE_DICT.update(OTHER_TYPE_DICT)


def pare_columns_in_df(df, reorder_columns):
    existing_columns = df.columns
    all_columns = []
    all_columns.extend(existing_columns)
    all_columns.extend(reorder_columns)
    all_columns = list(set(all_columns))
    reorder_columns_not_in_existing = [
        i for i in reorder_columns
        if i not in existing_columns]
    existing_not_in_reorder_columns = [
        i for i in existing_columns
        if i not in reorder_columns]
    if len(reorder_columns_not_in_existing) > 0:
        logging.warning(str(reorder_columns_not_in_existing))
        # Add columns if they don't exist already
        for column_name in reorder_columns_not_in_existing:
            df[column_name] = np.nan
            logging.info('adding column: ' + column_name)
    if len(existing_not_in_reorder_columns) > 0:
        logging.warning(str(existing_not_in_reorder_columns))
    for column in existing_not_in_reorder_columns:
        del df[column]
        logging.warning('Deleting column: ' + column)

def reorder_columns_in_df(df, reorder_columns):
    existing_columns = df.columns
    all_columns = []
    all_columns.extend(existing_columns)
    all_columns.extend(reorder_columns)
    all_columns = list(set(all_columns))
    reorder_columns_not_in_existing = [
        i for i in reorder_columns
        if i not in existing_columns]
    existing_not_in_reorder_columns = [
        i for i in existing_columns
        if i not in reorder_columns]
    if len(reorder_columns_not_in_existing) > 0:
        logging.warning(str(reorder_columns_not_in_existing))
        # Add columns if they don't exist already
        for column_name in reorder_columns_not_in_existing:
            df[column_name] = np.nan
            logging.info('adding column: ' + column_name)
    if len(existing_not_in_reorder_columns) > 0:
        logging.warning(str(existing_not_in_reorder_columns))
    for column in existing_not_in_reorder_columns:
        del df[column]
        logging.warning('Deleting column: ' + column)
    logging.info('Reordering columns')
    try:
        df = df[reorder_columns]
        # .columns = reorder_columns
    except:
        import pdb; pdb.set_trace()
    return df


def say_hello(*args, **kwargs):
    print('Hello world')
    return 'hi'



def execute_sql(*args, sql=None, database_id=None):
    if sql is None:
        raise Exception('No SQL to execute')
    if database_id is None:
        raise Exception('Need a `database_id`')
    fut = civis.io.query_civis(sql, database=database_id)
    result = fut.result()


def combine_denormalized_tables(pipeline_step, rename_columns=None, **kwargs):
    df_list = pipeline_step.pipeline.step_dict['get_denormalized_household_attempts'].output.values()
    df_list = [df.rename(columns={'Districtnumber': 'DistrictNumber', 'source': 'Source', 'Targetphone': 'TargetPhone'}) for df in df_list if not df.empty]
    df = pd.concat(
        df_list,
        axis=0,
        ignore_index=True)
    rename_columns = rename_columns or {}
    df.rename(columns=rename_columns, inplace=True)
    return df


def combine_tables(pipeline_step, deduplicate=False, columns=None, previous_step=None, **kwargs):
    if previous_step is None:
        raise Exception('Must provide previous step to `combine_tables`')
    df_list = pipeline_step.pipeline.step_dict[
        previous_step].output.values()
    df_list = list(df_list)
    if columns is None:
        df_list_pared = [df for df in df_list if not df.empty]
    else:
        for column in columns:
            for df in df_list:
                if column not in df.columns:
                    logging.info('adding a missing column {column}'.format(column=column))
                    df[column] = np.nan
        try:
            df_list_pared = [df[columns] for df in df_list if not df.empty]
        except:
            logging.error(str(columns), str([df.columns for df in df_list]))
            raise Exception('crap')

    df = pd.concat(df_list_pared, axis=0, ignore_index=True)
    if deduplicate:
        df = df.drop_duplicates(subset='attempt_id')
    return df


def make_narrow_table(pipeline_step, pause=False, rename_columns=None, **kwargs):
    '''
    Loop through the whole denormalized table and extract all the values, etc.
    '''
    denormalized_table = pipeline_step.pipeline.step_dict['aggregate_denormalized_table'].output[0]
    denormalized_table = denormalized_table.drop_duplicates('attempt_id')
    rows_dict = {}
    row_index = 0
    for row in denormalized_table.iterrows():
        row_dict = row[1].to_dict()
        hash_string = row_dict['attempt_id']
        for key, value in row_dict.items():
            if key == 'attempt_id':
                continue
            value = str(value)
            if value.lower() == 'nan':
                continue
            if key.lower() != key:  # Tricky
                continue
            one_row = [
                hash_string,
                key,
                value]
            # row_series = pd.Series(one_row)
            rows_dict[row_index] = one_row
            row_index += 1
            if row_index % 10000 == 0:
                logging.info('narrow table row ' + str(row_index))
    df = pd.DataFrame(list(rows_dict.values()))
    df.rename(columns={0: 'attempt_id', 1: 'attribute', 2: 'value'}, inplace=True)
    df['et_created'] = np.nan
    if pause:
        import pdb; pdb.set_trace()
    return df

def flatten(nested_list):
    output = []
    def _inner(thing):
        if not isinstance(thing, list):
            output.append(thing)
        else:
            for item in thing:
                _inner(item)
    _inner(nested_list)
    return output


class MyExeption(Exception):
    '''
    Not sure if this will be useful. Placeholder for now.
    '''
    pass


def succeeded(response):
    '''
    Tests the response code in the HTTP response to see if it's in the 200s.
    '''
    return (
        response.status_code >= 200 and response.status_code < 300)


def is_primitive_type(thing):
    '''
    Returns whether the `thing` is a Python primitive type -- i.e. not a `list`
    or a `dict`.

    Arguments:
        thing: The object being tested

    Returns:
        bool: `True` if it's primitive; `False` otherwise
    '''
    return type(thing) in PRIMITIVE_TYPE_DICT


def json_type(thing):
    '''
    Gives you the JSON type (e.g. `array`) for the type of the `thing`.

    Arguments:
        thing: Any object for which we want a JSON type

    Returns:
        str: The JSON type of the `thing`
    '''
    return JSON_TYPE_DICT[type(thing)]


def is_array(thing):
    '''
    Tells you if the `thing` is a `list`.

    Arguments:
        thing: The object being tested

    Returns:
        bool: `True` if `thing` is a list; `False` otherwise.
    '''
    return isinstance(thing, list)


def is_object(thing):
    '''
    Tells you if the `thing` is a dictionary (aka a JSON "object").

    Arguments:
        thing: The object being tested

    Returns:
        bool: `True` if the `thing` is a dictionary
    '''
    return isinstance(thing, dict)


def add_headers(json_schema):
    '''
    Adds a couple of keys that are used in JSON schema.

    Arguments:
        json_schema: A JSON schema as a dictionary

    Returns:
        None (in-place method)
    '''
    json_schema['$schema'] = 'SCHEMA'
    json_schema['id'] = 'ID'


def make_json_schema_from_sample(obj, headers=True):
    '''
    Takes a dictionary or list-like object and creates a JSON schema
    for it.

    Arguments:
        obj: A dictionary-like object for which we are creating the schema

    Returns:
        dict: A JSON schema as a dictionary
    '''
    def _inner(thing):
        if is_primitive_type(thing):
            return {'type': json_type(thing), '__name__': None}
        elif is_array(thing):
            return {
                'type': 'array',
                'items': [_inner(item) for item in thing]}
        elif is_object(thing):
            return {'type': 'object', 'properties': {
                sub_key: _inner(sub_value)
                for sub_key, sub_value in thing.items()}}
        else:
            raise Exception('Unrecognized type in the object.')
    schema = _inner(obj)
    if headers:
        add_headers(schema)
    return schema


def say_goodbye(*args, **kwargs):
    '''
    It's nice to say "goodbye" when you're exiting. Used for testing only.
    '''
    print('Goodbye!')


class ListIndex:
    '''
    This class is really just a wrapper for a `list`. Might be overkill, but
    (a) it's useful to have a class like this for instance type checking, and
    (b) I suspect we'll want to add more functionality to this class
    eventually.
    '''
    def __init__(self, number):
        self.index = number

    def __repr__(self):
        return str(self.index)

def has_mustache(obj):
    '''
    Tests whether the `obj` is a string that has a mustache on it. In JSON,
    we use `__` instead of mustaches due to parsing issues. But we call `__`
    a mustache as well.

    Arguments:
        obj: The string which may or may not have a mustache

    Returns:
        bool: `True` if it has a mustache, `False` otherwise
    '''
    return isinstance(obj, str) and (
        (obj[:2] == '{{' and obj[-2:] == '}}') or
        (obj[:2] == '__' and obj[-2:] == '__'))


def locate_stuff(obj, test_function):
    '''
    Returns a list of keypaths in the object (which is either dictionary- or
    list-like) for which the `test_function` returns `True`.

    Arguments:
        obj: The dictionary- or list-like object

    Returns:
        `list` of `ListIndex`
    '''
    results = []
    def _inner(obj, path=None):
        path = path or []  # Don't use mutable things as default kwarg values
        if isinstance(obj, (list,)):
            for index, item in enumerate(obj):
                _inner(item, path=path + [ListIndex(index)])
        elif isinstance(obj, (dict,)):
            for key, value in obj.items():
                _inner(value, path=path + [key])
        elif test_function(obj):
            results.append(path)
        else:
            pass
    _inner(obj)
    return results


def replace_stuff(obj, test_function, replace_function):
    '''
    Traverses a nested structure like a dictionary (which may contain other
    dictionaries or lists). It finds all the elements for which `test_function`
    evaluates to `True`, and replaces such elements with the output of
    `replace function` (which takes the element as an argument).

    Arguments:
        obj: The nested structure being traversed.
        test_function: A function used to identify which elements are to be
            replaced
        replace_function: The function that generates the replacement value
            the elements

    Returns:
        None: This is an in-place function
    '''
    locations = locate_stuff(obj, test_function)

    def _get_location(tmp_obj, path_list):
        for path_item in path_list:
            if isinstance(path_item, ListIndex):
                tmp_obj = tmp_obj[path_item.index]
            else:
                tmp_obj = tmp_obj[path_item]
        return tmp_obj

    for path in locations:
        sub_obj = _get_location(obj, path[:-1])
        last_path = (
            path[-1] if not isinstance(path[-1], ListIndex)
            else path[-1].index)
        last_path_item = sub_obj[last_path]
        sub_obj[last_path] = replace_function(last_path_item)


def get_named_keypaths(obj):
    results = []
    def _inner(obj, path='.'):
        if (
            isinstance(obj, dict) and '__name__' in obj and
                obj['__name__'] is not None):
            results.append((tuple(path.split('|')[1:]), obj['__name__'],))
        elif isinstance(obj, dict) and obj['type'] == 'object':
            for key, value in obj['properties'].items():
                _inner(value, path='|'.join([path, key]))
        elif isinstance(obj, dict) and obj['type'] == 'array':
            for index, item in enumerate(obj['items']):
                index = str(index)
                _inner(item, path='|'.join([path, index]))
        else:
            pass
    _inner(obj)
    return results


def add_hash_column(df, hash_column_name='hash', max_digits=9):
    '''
    Adds a column to a DataFrame that contains a hash of the other columns.

    Arguments:
        df: The dataframe
        hash_column_name: The name of the new column containing the hash.

    Returns:
        None: Modifies the dataframe in-place.
    '''
    df[hash_column_name] = df.apply(
        lambda x: int_hash(x, max_digits=max_digits),
        axis=1)


def int_hash(x, max_digits=32):
    '''
    Makes a hash that's an integer.

    Arguments:
        x: hashable thing

    Returns:
        int: Hash of `x`
    '''
    if isinstance(x, str):
        x = ''.join([c for c in x.lower() if c in (string.ascii_letters + string.digits)])

    bytes_string = pickle.dumps(x)
    value = hashlib.md5(bytes_string).hexdigest()
    #value = value % (10 ** (max_digits - 1))
    return value

def de_questionize(
    output, create_hash_column=True, hash_column_name='hash',
        erase_question_columns=False, rename_columns=None, **kwargs):
    '''Going to move this to a hubdialer script'''
    logging.info('Rearranging question columns from household_attempts')
    rename_columns = rename_columns or {}
    df = to_dataframe(output, create_hash_column=create_hash_column, hash_column_name=hash_column_name, **kwargs)
    question_columns = [
        column_name for column_name in df.columns
        if '?' in column_name or len(column_name) > 32]
    non_question_columns = [
        column_name for column_name in df.columns
        if column_name not in question_columns]
    question_hashes = {question:
        int_hash(question, max_digits=8) for question in question_columns}
    rows_dict = {}  # using a dict for speed
    index_counter = 0
    for row in df.iterrows():
        for question_column in question_columns:
            series = row[1].to_dict()
            question_hash = question_hashes[question_column]
            response = series[question_column]
            series['question_id'] = question_hash
            series['question'] = question_column
            series['response'] = series[question_column]
            if erase_question_columns:
                series = {key: value for key, value in series.items() if '?' not in key}
            rows_dict[index_counter] = series
            index_counter += 1
    newdf = pd.DataFrame(list(rows_dict.values()))
    newdf.rename(columns=rename_columns, inplace=True)
    if len(question_columns) > 1:
        pass
    if not newdf.empty:
        # newdf.drop(columns=question_columns, inplace=True)
        logging.info('New dataframe is not empty')
        newdf['call_date'] = newdf.apply(lambda row: row.time_parsed.date(), axis=1)
        newdf['call_time'] = newdf.apply(lambda row: row.time_parsed.time(), axis=1)
    else:
        logging.warning('New dataframe is empty')
    return newdf


def to_dataframe(
    output, create_hash_column=False,
    hash_column_name='attempt_id', ensure_columns_exist=None,
    rename_columns=None, split_date_column=None, pause=False, cast_column_types=None,
    replacement_columns=None, replacement_regex='',
        replacement_value='', max_digits=8, reorder_columns=None, **kwargs):
    '''
    Transform string into pd.DataFrame
    '''
    replacement_columns = replacement_columns or []
    cast_column_types = cast_column_types or {}
    rename_columns = rename_columns or {}
    ensure_columns_exist = ensure_columns_exist or []
    file_like_obj = io.StringIO(output)

    df = pd.read_csv(file_like_obj, **kwargs, engine='python')
    df.rename(columns=rename_columns, inplace=True)
    for column_name, default_value in ensure_columns_exist:
        try:
            df.insert(len(df.columns), column_name, default_value)
        except:
            logging.warning(
                'column {column_name} already exists'.format(
                    column_name=column_name))
    if split_date_column is not None:

        def reformat_date(row, index=0):
            bad_dates = row.dates
            try:
                bad_date = bad_dates.split(' - ')[index]
            except IndexError:
                # Happens when the "dates" column has only one date.
                logging.info('Cannot parse date: {d}'.format(d=str(bad_dates)))
                return None
            month, day, year = bad_date.split('/')
            month = '0' + month if len(month) == 1 else month
            day = '0' + day if len(day) == 1 else day
            new_date = '-'.join([year, month, day])
            return new_date

        #start_date, end_date = df[split_date_column][0].split(' - ')
        #start_date = reformat_date(start_date)
        #end_date = reformat_date(end_date)

        df['start_date'] = df.apply(lambda x: reformat_date(x, 0), axis=1)
        df['end_date'] = df.apply(lambda x: reformat_date(x, 1), axis=1)

    if create_hash_column:
        add_hash_column(df, hash_column_name=hash_column_name, max_digits=max_digits)
        logging.info('Adding a hash column to the dataframe')
    for replacement_column in replacement_columns:
        try:
            df[replacement_column] = df[replacement_column].str.replace(
                replacement_regex, replacement_value, regex=True)
        except AttributeError:
            pass


    if reorder_columns is not None:
        df = reorder_columns_in_df(df, reorder_columns)
    for column_name, type_name in cast_column_types.items():
        if column_name not in df.columns:
            logging.info('Skipping recast of column {column}'.format(column=column_name))
            continue
        if type_name[:3] == 'int':
            df[column_name] = df[column_name].fillna(0).astype(type_name)
        else:
            df[column_name] = df[column_name].astype(type_name)
    logging.info('returning df')
    if pause:
        import pdb; pdb.set_trace()


    return df


#def rename_columns(df, column_mapping):
#    df.rename(columns=column_mapping, inplace=True)


def get_from_keypath(obj, keypath):
    for key in keypath:
        obj = obj[key]
    return obj


def blobify(func, schema):
    function_params = inspect.signature(func).parameters
    named_keypaths = get_named_keypaths(schema)
    keypath_dict = {name: keypath for keypath, name in named_keypaths}

    def _blobify(blob):
        splatted_args = []
        for function_param in function_params:
            keypath = keypath_dict[function_param]
            value = get_from_keypath(blob, keypath)
            splatted_args.append(value)
        return func(*splatted_args)

    globals()[func.__name__] = _blobify


def read_config(config_path):
    '''
    Reads a YAML config.

    Arguments:
        config_path: Path to the YAML file

    Returns:
        dict: Config file as a dictionary
    '''
    return pyaml.yaml.load(open(config_path, 'r'))


def import_function(function_path, iterable=False):
    # If the module isn't specified, assume it's this one
    if function_path is not None and '.' not in function_path:
        function = getattr(APIPipelineStep, function_path)
    elif function_path is not None:
        module_name, function_name = function_path.split('.')
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
    else:
        function = lambda x: None
        if iterable:
            function = iter([])
    return function


class APIPipeline:
    '''
    The `APIPipeline` contains all the steps necessary for interacting with
    an API via GET or POST requests. It is initialized with a `config_dict`
    containing a top-level `session` key, with `steps` as elements in a list.
    Each `step` is bassed to the `APIPipelineStep` constructor.

    This class also holds values and intermediate results that are generated
    by an `APIPipelineStep` and which could be useful for later steps in the
    pipeline.
    '''

    def __init__(self, config_dict):
        mustaches_to_environment_variables(config_dict)
        self.steps = [
            APIPipelineStep(config) for config in config_dict['session']]
        self.step_dict = {}
        for step in self.steps:
            step.pipeline = self
            self.step_dict[step.step_name] = step
        self.before_config = config_dict.get('before', {})
        self.after_config = config_dict.get('after', {})
        self.after_function_name = self.after_config.get(
            'function_name', 'do_nothing')
        self.after_function = import_function(self.after_function_name)
        self.after_function_kwargs = self.after_config.get('kwargs', {})

    def execute_pipeline(self):
        '''
        This runs the steps in the `APIPipeline`.
        '''
        self.session = requests.session()
        for step in self.steps:
            step.execute_step()
        # If there's an "after" function specified in the config, execute it.
        self.after_function(self, **self.after_function_kwargs)


def do_nothing(obj, **kwargs):
    '''
    When optional functions do not occur in an `APIPipelineStep` object,
    this function is called. It doesn't do anything except return its input.
    '''
    return obj


class APIPipelineStep:
    '''
    This is the object that holds all configuration information to actually
    hitting the API and doing something to the results.
    '''
    def __init__(self, config_dict):
        # Get the configuration dictionary and replace for environment vars
        self.config_dict = config_dict
        mustaches_to_environment_variables(self.config_dict)

        # Should we skip the step completely?
        self.skip = config_dict.get('skip', False)

        # We will always need a bail-out for kludgey stuff
        self.function_config = config_dict.get('function', {})
        self.function_name = self.function_config.get('function_name', None)
        self.function_kwargs = self.function_config.get('kwargs', {})
        self.function = import_function(self.function_name)

        self.step_name = config_dict['step_name']
        self.url = config_dict.get('url', None)
        self.request_type = config_dict.get('request_type', None)

        # Do we pass any cookies to the following step?
        self.pass_cookies = config_dict.get('pass_cookies', False)

        # Explicitly defined POST parameters are here
        self.post_parameters = config_dict.get('post_parameters', {})

        # Do we substitute strings for parts of the URL?
        self.endpoint_parameters = config_dict.get('endpoint_parameters', None)

        # Function to send output through immediately after we get it
        # This could be used for parsing/cleaning output
        self.raw_data_parser_config = config_dict.get('raw_data_parser', {})
        self.raw_data_parser_function_name = self.raw_data_parser_config.get(
            'function_name', 'utils.do_nothing')
        self.raw_data_parser_function = import_function(
            self.raw_data_parser_function_name)
        self.raw_data_parser_kwargs = self.raw_data_parser_config.get(
            'kwargs', {})

        # Do we store the output in a list or dict?
        self.store_output_config = config_dict.get('store_output', {})
        self.store_output_struct = self.store_output_config.get('struct', None)
        self.store_output_key = self.store_output_config.get('key', None)

        # Function called on the output after it has been through the (optional)
        # `parse_output` function. This would be used (e.g.) to send the output
        # to Redshift.
        self.output_function_config = config_dict.get('output_function', {})
        self.output_function_name = self.output_function_config.get(
            'function_name', None)
        self.output_function = import_function(self.output_function_name)
        self.output_function_kwargs = self.output_function_config.get(
            'kwargs', {})

        # Do we obtain endpoint parameters from a function? If so, it's
        # specified here. We assume that the function returns a `list` of
        # `dict`, which will be passed through a `.format()` method.
        self.endpoint_parameters_function_config = config_dict.get(
            'endpoint_parameters_function', {})
        self.endpoint_parameters_function_name = \
            self.endpoint_parameters_function_config.get('function_name', None)
        self.endpoint_parameters_function_kwargs = \
            self.endpoint_parameters_function_config.get('kwargs', {})

        # Just an empty default in case we're kludging
        self.current_endpoint_dict = {}

        # This is where we actually load the function. We assume that the
        # function is given in a string of the form "module.function_name".
        self.endpoint_parameters_function = import_function(
            self.endpoint_parameters_function_name, iterable=True)

        # If specified, we store the output in either a `list` or a `dict`
        # that is keyed to one of the parameters.
        if self.store_output_struct == 'list':
            self.output = []
        elif self.store_output_struct == 'dict':
            self.output = collections.defaultdict(dict)
        else:
            self.output = None
        self.key_value = {}  # for storing params

    @property
    def step_index(self):
        this_index = [
            index for index, step in enumerate(self.pipeline.steps)
            if step.step_name == self.step_name][0]
        return this_index

    @property
    def previous_step(self):
        return self.pipeline.steps[self.step_index - 1]

    def from_previous_step(self, **kwargs):
        if 'step_name' not in kwargs or kwargs['step_name'] is None:
            previous_step = self.previous_step
        else:
            previous_step = self.pipeline.step_dict[kwargs['step_name']]
        if (isinstance(previous_step.output, (list, tuple,)) and
                not isinstance(previous_step.output[0], dict)):
            if 'keyname' not in kwargs:
                raise Exception('Need to specify a keyname')
            output = [
                {kwargs['keyname']: output} for output in previous_step.output]
        else:
            output = previous_step.output
        return output


    def execute_step(self):
        '''
        This called to execute the `APIPipelineStep` object. It executes
        either a POST or a GET depending on the configuration.
        '''
        if self.skip:
            logging.info('Skipping step ' + self.step_name)
            return

        logging.info(
            'Executing step: {step_name}'.format(step_name=self.step_name))
        if self.url is None and self.function is not None:
            response = [self.function(self, **self.function_kwargs)]
        elif self.request_type.lower() == 'post':
            response = self.execute_post_request()
        elif self.request_type.lower() == 'get':
            response = self.execute_get_request()
        else:
            raise Exception(
                'Unknown request type: {request_type}'.format(
                    request_type=self.request_type))

        # Handle the response, if specified
        # Note that we may have executed several requests, the output of
        # which are stored in an iterable called `response`. This is why
        # we're looping through them.

        # It is an assumption in this flow that the requests do not depend
        # on each other. If they do, then they need to be in separate
        # `APIPipelineStep` objects.
        for one_response in response:
            # Send the response to `raw_data_parser_function`
            one_response = self.raw_data_parser_function(
                one_response, **self.raw_data_parser_kwargs)
            # Store the output if the user has specified
            if self.store_output_struct == 'list':
                self.output.append(one_response)
            elif self.store_output_struct == 'dict':
                self.output[self.key_value[self.store_output_key]] = one_response
            else:
                pass  # No output is being stored
            logging.info(
                'Sending output of step {step_name} '
                'to {output_function_name}'.format(
                    step_name=self.step_name,
                    output_function_name=str(self.output_function_name)))
            # Finally, send the (possibly parsed) output to the
            # `output_function`.
            # self.substitute_parameters_dict(self.output_function_kwargs)
            if not self.output_function_config.get('not_really', False):
                self.output_function(
                    one_response,
                    **(self.substitute_parameters_dict(
                        self.output_function_kwargs)))
            else:
                logging.warning('Skipping this step due to `not_really` flag')
        if (self.store_output_struct == 'list' and
                self.store_output_config.get('flatten', False)):
            self.output = flatten(self.output)

    def execute_post_request(self):
        '''
        This does not have feature parity with `execute_get_request`.
        '''
        post_response = self.pipeline.session.post(
            self.url, data=self.post_parameters)
        if self.pass_cookies:
            self.pipeline.cookies = post_response.cookies
        yield post_response

    def execute_get_request(self):
        '''
        The work here is to identify where any parameters are coming from and
        substitute them as necessary, possibly looping through a set of
        parameters if necessary.
        '''
        # Are the endpoint parameters specified explicitly in the configuration
        # file? If so, get them.
        if self.endpoint_parameters is not None:
            endpoint_dict_list = self.endpoint_parameters
        # Is there a function that's supposed to return the endpoint
        # parameters? If so, execute it and put the results into the
        # `endpoint_dict_list`.

        # Is the function a method that's built into `APIPipelineStep`?
        if (hasattr(self, self.endpoint_parameters_function.__name__) and
            isinstance(getattr(self, self.endpoint_parameters_function.__name__),
                types.MethodType)):
            endpoint_dict_list = self.endpoint_parameters_function(
                self, **self.endpoint_parameters_function_kwargs)
        # No it isn't
        elif self.endpoint_parameters_function is not None:
            logging.info('Have endpoint parameters function')
            endpoint_dict_list = self.endpoint_parameters_function(
                **self.endpoint_parameters_function_kwargs)
        else:
            # Not sure if we really want this case. GET request may be static
            raise Exception(
                'Need either endpoint_parameters or '
                'endpoint_parameters_function to be defined.')
        # We will always loop through endpoint parameters; so if it isn't an
        # iterable thing, then make it a singleton list.
        if not isinstance(endpoint_dict_list, (list, tuple,)):
            endpoint_dict_list = [endpoint_dict_list]
        # Finally, hit the parameterized endpoint and yield back the results
        for endpoint_dict in endpoint_dict_list:
            self.current_endpoint_dict = endpoint_dict
            get_response = self.pipeline.session.get(
                self.url.format(**endpoint_dict),
                cookies=self.pipeline.cookies)
            self.key_value.update(endpoint_dict)
            yield get_response.text

    def substitute_parameters(self, some_string):
        '''
        For subsituting parameters into {parameter} strings.
        '''
        if not isinstance(some_string, (str,)):
            return some_string
        for parameter, value in self.current_endpoint_dict.items():

            template_string = '{' + parameter + '}'
            some_string = some_string.replace(template_string, value)
        return some_string

    def substitute_parameters_dict(self, some_dict):
        some_dict_copy = copy.deepcopy(some_dict)
        for key, value in some_dict.items():
            some_dict_copy[key] = self.substitute_parameters(value)
        return some_dict_copy


def environment_variables(some_string):
    '''
    Takes a string with a mustache (or double underscore), shaves it, and
    returns the corresponding environment variable.

    Arguments:
        some_string: The string that is of the form `{{environment_variable}}`

    Returns:
        str: The environment variable value
    '''
    return os.environ[some_string[2:-2]]


def mustaches_to_environment_variables(config_dict):
    '''
    Traverses a configuration (or other dictionary) and replaces all the
    mustaches with the values of the corresponding environment variable.

    Arguments:
        config_dict: The configuration (or other `dict`)

    Returns:
        None: The dictionary is modified in-place.
    '''
    replace_stuff(config_dict, has_mustache, environment_variables)


def to_redshift(
    output, database_id=None, schema_name=None, table_name=None,
    cast_column_types=None, skip=False, pare_columns=None, skip_empty=True, pause=False,
        existing_table_rows='truncate', reorder_columns=None, **kwargs):
    logging.info('Sending data to table: ' + table_name)
    logging.info('Existing rows policy: ' + existing_table_rows)
    if database_id is None:
        raise Exception('Need a database ID')
    if output.empty and skip_empty:
        logging.info('Skipping upload of empty table')
        return
    if skip:
        logging.info('Skipping insert to Redshift `skip=True`')
        return
    cast_column_types = cast_column_types or {}
    client = civis.APIClient()
    db_id = client.get_database_id(database_id)
    if table_name is None:
        raise Exception('Need a table name')

    if pare_columns is not None:
        pare_columns_in_df(output, pare_columns)
    if reorder_columns is not None:
        output = reorder_columns_in_df(output, reorder_columns)
    if pause:
        import pdb; pdb.set_trace()

    for column_name, type_name in cast_column_types.items():
        if column_name not in output.columns:
            logging.info('Skipping recast of column {column}'.format(column=column_name))
            continue
        if type_name[:3] == 'int':
            output[column_name] = output[column_name].fillna(0).astype(type_name)
        else:
            output[column_name] = output[column_name].astype(type_name)

    future = civis.io.dataframe_to_civis(
        output, db_id, table=schema_name + '.' + table_name,
        existing_table_rows=existing_table_rows, headers=True, hidden=False)
    if kwargs.get('block', False):
        logging.info('blocking due to `block=True`')
        result = future.result()
        print(result)



if __name__ == '__main__':
    pass

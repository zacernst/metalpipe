'''
Location for specific types of nodes.
'''
import logging
import random
import copy
import uuid
import time
import requests
from nanostream_trigger import Trigger
from nanostream_processor import (
    NanoStreamListener, NanoStreamSender,
    NanoAncestor, NanoStreamProcessor,
    exception)


class ChaosMonkey(NanoStreamProcessor):
    '''
    With some probability, raise an Exception.
    '''
    def __init__(self, probability=.01):
        self.probability = probability
        super(ChaosMonkey, self).__init__()

    def process_item(self, item):
        if random.random() < self.probability:
            raise Exception('Monkey')
        return item


class Filter(NanoStreamProcessor):
    '''
    Pass through the message if a test [is/is not] passed.
    '''
    def __init__(self, module_name, function_name, pass_if=True):
        self.module = importlib.import_module(module_name)
        self.function = self.module.getattr(function_name)
        self.pass_if = pass_if
        super(Filter, self).__init__()

    def process_item(self, item):
        if self.function(item) == self.pass_if:
            return item
        return None


class DoNothing(NanoStreamProcessor):
    '''
    Just a pass-through for testing and stuff.
    '''
    def process_item(self, item):
        logging.info('Saw an item.')
        return item


class PrintStreamProcessor(NanoStreamProcessor):
    """
    Just a class that prints, for testing purposes only.
    """
    def process_item(self, item):
        print(item)
        return item


class ExtractKeysStreamProcessor(NanoStreamProcessor):
    """
    Just extracts the keys from a dictionary. For testing.
    """
    def process_item(self, item):
        output = list(item.keys())
        return output


class CounterOfThings(NanoStreamSender):

    def start(self):
        '''
        Just start counting integers
        '''
        counter = 0
        while 1:
            self.queue_output(counter)
            counter += 1


class StringSplitter(NanoStreamProcessor):
    def __init__(self, delimiter=',', message_key=None):
        self.delimiter = delimiter
        self.message_key = message_key
        super(StringSplitter, self).__init__()

    def process_item(self, message):
        input_string = (
            message if self.message_key is None
            else message[self.message_key])
        for item in input_string.split(self.delimiter):
            pass


class SendEnvironmentVariables(NanoStreamProcessor):

    def __init__(self, variable_list=None):
        variable_list = variable_list or exception(
            message='Need a list of variables.')
        self.variable_list = variable_list
        super(SendEnvironmentVariables, self).__init__()

    def process_item(self, message):
        variables = {
            key: os.environ[key] for key in self.variable_list}
        return variables


class Kickoff(NanoStreamSender):

    def start(self):
        self.queue_output(NanoStreamTrigger())


class DivisibleByThreeFilter(NanoStreamProcessor):

    def process_item(self, message):
        if message % 3 == 0:
            return message


class DivisibleBySevenFilter(NanoStreamProcessor):

    def process_item(self, message):
        if message % 7 == 0:
            return message


class PrinterOfThings(NanoStreamProcessor):

    def __init__(self, prepend=''):
        self.prepend = prepend
        super(PrinterOfThings, self).__init__()

    def process_item(self, message):
        print(self.prepend + str(message))
        return message


class MakeHttpSession(NanoStreamProcessor):
    '''
    Job is to create a session upon initialization and pass it to the
    `NanoStreamGraph.global_dict`.
    '''
    def __init__(self, session_key='http_session', **session_kwargs):
        self.session = requests.session(**session_kwargs)
        self.session_key = session_key
        super(MakeHttpSession, self).__init__()

    def pre_flight_check(self):
        self._make_global(self.session_key, self.session)


class HttpGetRequest(NanoStreamProcessor):
    '''
    This is the object that holds all configuration information to actually
    hitting the API and doing something to the results.
    '''
    def __init__(
        self, url=None, url_parameter_dict=None, url_parameter_key=None,
            session_key=None, json_output=False, **kwargs):
        '''
        Keep the functionality of this module very minimal.
        '''
        self.url = url
        self.url_parameter_dict = url_parameter_dict or {}
        self.url_parameter_key = url_parameter_key
        self.session = requests.Session()
        self.session_key = session_key or uuid.uuid4().hex
        self.json_output = json_output
        super(HttpGetRequest, self).__init__(**kwargs)

    def pre_flight_check(self):
        pass

    def process_item(self, message):
        '''
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a dictionary
        with keys and values to be passed in the GET request.

        ```
        {'url': 'http://www.foobar.com/{param}',
         'param': 1}
        '''

        # Hit the parameterized endpoint and return the results
        if isinstance(message, dict):
            self.url_parameter_dict.update(message)
        elif isinstance(message, Trigger):
            pass
        else:
            self.url_parameter_dict[self.url_parameter_key] = message
        get_response = self.session.get(
            self.url.format(**self.url_parameter_dict)
            )
        logging.info(
            'GET request to: ' + self.url.format(**self.url_parameter_dict))
        return get_response.text


class HttpPostRequest(NanoStreamProcessor):
    '''
    This is the object that holds all configuration information to actually
    hitting the API and doing something to the results.
    '''
    def __init__(
        self, url=None, session_key='http_session', post_data=None,
            json_output=False, **kwargs):
        '''
        Keep the functionality of this module very minimal.
        '''
        self.url = url
        self.session_key = session_key
        self.post_data = post_data or {}
        super(HttpPostRequest, self).__init__(**kwargs)

    def pre_flight_check(self):
        self.session = self.get_global(self.session_key)
        while self.session is None:
            self.session = self.set_global(self.session_key, default={})
        logging.info('Got session')

    def process_item(self, message):
        '''
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a dictionary
        with keys and values to be passed in the GET request.

        ```
        {'url': 'http://www.foobar.com/{param}',
         'param': 1}
        '''
        # Hit the parameterized endpoint and return the results
        post_data = copy.deepcopy(self.post_data)
        if isinstance(message, dict):
            post_data.update(dict)
        logging.info('POST: ' + self.url)
        post_response = self.session.post(
            self.url, data=post_data)
        # logging.info('POST response: ' + post_response.text)
        return post_response.text


class Serializer(NanoStreamProcessor):
    '''
    Takes an iterable and sends out a series of messages while iterating
    over it.
    '''
    def __init__(self, include_batch_markers=False):
        self.include_batch_markers = include_batch_markers
        super(Serializer, self).__init__()

    def process_item(self, message):
        if self.include_batch_markers:
            self.queue_output(BatchStart())
        for item in message:
            self.queue_output(item)
        if self.include_batch_markers:
            self.queue_output(BatchEnd())


class Bundler(NanoStreamProcessor):
    '''
    For taking a series of things and putting them into some kind of single
    structure. Listens for `Batch` objects to tell it when to start and stop.
    '''
    def __init__(self):
        self.batch = []
        self.accepting_items = False
        super(Bundler, self).__init__()

    def process_item(self, message):
        if isinstance(message, BatchStart):
            self.accepting_items = True
            self.batch = []
        elif isinstance(message, BatchEnd):
            self.accepting_items = False
            return self.batch
        else:
            if self.accepting_items:
                self.batch.append(message)
            else:
                pass


class ConstantEmitter(NanoStreamSender):
    '''
    Send a thing every n seconds
    '''
    def __init__(
            self, thing=None, thing_key=None, delay=None, from_json=False):
        if from_json:
            thing = json.loads(thing)

        self.thing = json.loads(thing) if from_json else thing
        self.thing_key = thing_key
        self.delay = delay or 0
        super(ConstantEmitter, self).__init__()

    def start(self):
        logging.debug('starting')
        while 1:
            time.sleep(self.delay)
            output = (
                {self.thing_key: self.thing} if self.thing_key is not None
                else self.thing)
            self.queue_output(output)


class Throttle(NanoStreamProcessor):
    '''
    Insert a delay into the pipeline.
    '''
    def __init__(self, delay=1):
        self.delay = delay

    def process_item(self, message):
        time.sleep(self.delay)
        return message


class ScheduledTrigger(NanoStreamSender):
    '''
    Sends a `Trigger` object periodically.
    '''
    def __init__(
        self, at_time_string='00:00',
            hours=None, minutes=None, seconds=None):

        super(ScheduledTrigger, self).__init__()

        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.at_time_string = at_time_string

        def send_the_trigger():
            '''
            We define this here so that we don't need to include arguments
            in the call to `schedule`'s `do()` function.
            '''
            self.queue_output(Trigger())

        numeric_interval = hours or minutes or seconds

        if hours:
            interval_type = 'hours'
        elif minutes:
            interval_type = 'minutes'
        elif seconds:
            interval_type = 'seconds'
        else:
            raise Exception('This should not happen')
        if numeric_interval is not None:
            getattr(
                schedule.every(numeric_interval),
                interval_type).do(send_the_trigger)


    def send_trigger(self):
        logging.info('sending a trigger...')
        self.queue_output(Trigger())


    def start(self):
        while 1:
            schedule.run_pending()
            time.sleep(.1)

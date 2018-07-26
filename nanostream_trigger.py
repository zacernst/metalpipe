'''
Sends a `Trigger` object downstream on a schedule.
'''

import time
import random
import datetime
import logging
import re
import hashlib
import schedule
from nanostream_pipeline import NanoStreamGraph
from nanostream_processor import (
    NanoStreamProcessor, NanoStreamSender)


logging.basicConfig(level=logging.INFO)


def hello_world():
    print('hello, world!')


class TimedTrigger:
    def __init__(self, previous_trigger_time=None, trigger_name=None):
        self.previous_trigger_time = None
        self.trigger_name = trigger_name or hashlib.md5(
            bytes(str(random.random()),'ascii')).hexdigest()
        self.time_sent = time.time()  # In epochs
        logging.info('Sent trigger at {now}'.format(
            now=str(datetime.datetime.now())))


class ScheduledTrigger(NanoStreamSender):
    '''
    Sends a `Trigger` object periodically.
    '''
    def __init__(
        self, day=None, at_time_string='00:00',
            hours=None, minutes=None, seconds=None):
        if not (day or hours or minutes or seconds):
            raise Exception('Need a parameter for the trigger job')
        if day and (hours or minutes or seconds):
            raise Exception(
                "Can't use `day` with `hours`, `minutes`, or `seconds`.")

        super(ScheduledTrigger, self).__init__()

        self.day = day
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.at_time_string = at_time_string

        def send_the_trigger():
            '''
            We define this here so that we don't need to include arguments
            in the call to `schedule`'s `do()` function.
            '''
            self.queue_output(TimedTrigger())

        numeric_interval = hours or minutes or seconds
        if day:
            interval_type = 'day'
        elif hours:
            interval_type = 'hours'
        elif minutes:
            interval_type = 'minutes'
        elif seconds:
            interval_type = 'seconds'
        else:
            raise Exception('This should not happen')
        if day:
            getattr(schedule.every(), day).at(self.at_time_string).do(hello_world)
        if numeric_interval is not None:
            getattr(
                schedule.every(numeric_interval),
                interval_type).do(send_the_trigger)


    def send_trigger(self):
        logging.info('sending a trigger...')
        self.queue_output(TimedTrigger())


    def start(self):
        while 1:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    import nanostream_pipeline
    pipeline = nanostream_pipeline.NanoStreamGraph()
    trigger_node = ScheduledTrigger(seconds=1)
    pipeline.add_node(trigger_node)
    pipeline.start()

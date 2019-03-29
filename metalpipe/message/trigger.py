"""
Trigger module
==============

A simple class containing no data, which is intended merely as a trigger,
signaling that the downstream node should do something.
"""

import time
import random
import datetime
import logging
import re
import hashlib
import schedule


def hello_world():
    print("hello, world!")


class Trigger:
    def __init__(self, previous_trigger_time=None, trigger_name=None):
        self.previous_trigger_time = None
        self.trigger_name = (
            trigger_name
            or hashlib.md5(bytes(str(random.random()), "ascii")).hexdigest()
        )
        self.time_sent = time.time()  # In epochs
        logging.debug(
            "Sent trigger at {now}".format(now=str(datetime.datetime.now()))
        )

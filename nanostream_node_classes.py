'''
Location for specific types of nodes.
'''
import logging
import random
import copy
import uuid
import time
import requests
from nanostream_processor import NanoStreamAncestor


class ExtractKeysStreamProcessor(NanoStreamAncestor):
    """
    Just extracts the keys from a dictionary. For testing.
    """
    def process_item(self):
        output = list(item.keys())
        yield output


class CounterOfThings(NanoStreamAncestor):

    def generator(self):
        '''
        Just start counting integers
        '''
        counter = 0
        while 1:
            yield counter
            counter += 1


class ConstantEmitter(NanoStreamAncestor):
    '''
    Send a thing every n seconds
    '''
    def __init__(
            self, thing=None, thing_key=None, delay=2, from_json=False):
        if from_json:
            thing = json.loads(thing)

        self.thing = json.loads(thing) if from_json else thing
        self.thing_key = thing_key
        self.delay = delay or 0
        super(ConstantEmitter, self).__init__()
        logging.debug('init constant emitter')

    def generator(self):
        logging.debug('starting constant emitter')
        while 1:
            time.sleep(self.delay)
            output = (
                {self.thing_key: self.thing} if self.thing_key is not None
                else self.thing)
            yield output
            logging.debug('yielded output: {output}'.format(output=output))


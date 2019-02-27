"""
"""

import pickle as pickle
import base64


def encode(something):
    """
    We encode all messages as base64-encoded pickle objects in case
    later on, we want to persist them or send them to another system.
    This is extraneous for now.
    """
    return base64.b64encode(pickle.dumps(something))


def decode(something):
    """
    Decodes from base-64 pickled object.
    """
    return pickle.loads(base64.b64decode(something))

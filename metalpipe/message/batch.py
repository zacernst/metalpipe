"""
Batch module
============

We'll use markers to delimit batches of things, such as serialized
files and that kind of thing.
"""

from metalpipe.utils.set_attributes import set_kwarg_attributes


class BatchStart:
    @set_kwarg_attributes()
    def __init__(self, *args, **kwargs):
        pass


class BatchEnd:
    @set_kwarg_attributes()
    def __init__(self, *args, **kwargs):
        pass

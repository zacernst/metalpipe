from timed_dict import TimedDict
from metalpipe_node import MetalNode


class InnerJoin(MetalNode):
    """
    Joins two streams of dict-like objects.
    """

    def __init__(self, join_keys=None, expiration_window=5):
        self.join_keys = join_keys

    def start(self):
        pass

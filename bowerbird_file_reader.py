import bowerbird
from nanostream_processor import NanoStreamProcessor

class BowerBirdFileReader(NanoStreamProcessor):
    def __init__(self, bowerbird_filesystem=None, read_mode='r', read_lines=False):
        self.bowerbird_filesystem = bowerbird_filesystem
        self.read_mode = read_mode
        self.read_lines = read_lines
        super(BowerBirdFileReader, self).__init__()

    def process_item(self, message):
        with self.bowerbird_filesystem.open(message, mode=self.read_mode) as f:
            data = f.readlines() if self.read_lines else f.read()
        return data

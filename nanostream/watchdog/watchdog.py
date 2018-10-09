# Borked!

'''
Experiment using `bowerbird` to set up a flexible watchdog job that
looks for new files to appear and sends their names downstream. Goal is
to have a single, configurable job that can watch files systems, sftp,
S3, GCP, etc.

'''

import time
import logging
import re
from nanostream_graph import NanoStreamGraph
from nanostream_processor import (
    NanoNode)
# from bowerbird.filesystem import LocalFileSystem


logging.basicConfig(level=logging.INFO)


class FileSystemWatchdog(NanoNode):
    def __init__(
        self, regexes=None, recurse=False,
            bowerbird_filesystem=None, poll_frequency=5):
        self.regexes = regexes or ['.*']
        self.have_seen = set()
        self.poll_frequency = poll_frequency
        self.bowerbird_filesystem = bowerbird_filesystem
        super(FileSystemWatchdog, self).__init__()

    def file_modified(self, *args, **kwargs):
        modified_file = args[0].src_path
        self.queue_message(modified_file)

    def process_item(self, tigger):
        seen = self.bowerbird_filesystem.ls('.')
        for filename in seen:
            logging.debug('checking:' + filename)
            if filename in self.have_seen:
                continue
            for regex in self.regexes:
                match = re.match(regex, filename)
                if match is not None:
                    self.have_seen.add(filename)
                    logging.info(
                        'See new object: {filename}'.format(
                            filename=filename))
                    self.queue_output(filename)


if __name__ == '__main__':
    # This watches your home directory for new CSV files and sends the
    # filename(s) to the next process in the pipeline.
    bowerbird_filesystem = LocalFileSystem()
    pipeline = NanoStreamGraph()
    watchdog = FileSystemWatchdog(
        regexes=['.*.csv$'],
        bowerbird_filesystem=bowerbird_filesystem)
    pipeline.add_node(watchdog)
    pipeline.start()

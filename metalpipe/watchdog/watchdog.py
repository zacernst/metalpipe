# Borked!
"""
Experiment using `bowerbird` to set up a flexible watchdog job that
looks for new files to appear and sends their names downstream. Goal is
to have a single, configurable job that can watch files systems, sftp,
S3, GCP, etc.

"""

import time
import logging
import re

# from bowerbird.filesystem import LocalFileSystem

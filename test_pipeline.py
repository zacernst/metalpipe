from nanostream_watchdog import FileSystemWatchdog
from nanostream_trigger import ScheduledTrigger
from nanostream_node_classes import (
    ChaosMonkey, PrinterOfThings, CounterOfThings)
from nanostream_graph import NanoStreamGraph
from bowerbird.filesystem import LocalFileSystem
from bowerbird_file_reader import BowerBirdFileReader
import logging


logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    pipeline = NanoStreamGraph()

    fs = LocalFileSystem()

    trigger = ScheduledTrigger(seconds=5)
    printer = PrinterOfThings(prepend='>>>')
    printer_1 = PrinterOfThings()
    chaos_monkey = ChaosMonkey()
    watchdog = FileSystemWatchdog(
        regexes=[r'.*.csv'],
        bowerbird_filesystem=fs)
    file_reader = BowerBirdFileReader(bowerbird_filesystem=fs)

    # pipeline + trigger + printer + watchdog + file_reader + printer_1
    # trigger > watchdog > printer
    # watchdog > file_reader > printer_1
    # pipeline.start()

    # pipeline + trigger + printer
    # trigger > printer

    pipeline_2 = NanoStreamGraph()
    counter = CounterOfThings()
    pipeline + counter + printer + chaos_monkey
    counter > chaos_monkey
    chaos_monkey > printer

    pipeline_2 + printer_1
    pipeline > pipeline_2





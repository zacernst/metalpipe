from nanostream_watchdog import FileSystemWatchdog
from nanostream_trigger import ScheduledTrigger
from nanostream_processor import PrinterOfThings
from nanostream_pipeline import NanoStreamGraph
from bowerbird.filesystem import LocalFileSystem
from bowerbird_file_reader import BowerBirdFileReader



if __name__ == '__main__':
    pipeline = NanoStreamGraph()

    fs = LocalFileSystem()
    trigger = ScheduledTrigger(seconds=5)
    printer = PrinterOfThings()
    printer_1 = PrinterOfThings()
    watchdog = FileSystemWatchdog(regexes=[r'.*.csv'], bowerbird_filesystem=fs)
    file_reader = BowerBirdFileReader(bowerbird_filesystem=fs)

    pipeline + trigger + printer + watchdog + file_reader + printer_1

    trigger > watchdog > printer
    watchdog > file_reader > printer_1


    # pipeline.start()

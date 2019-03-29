import sys
import logging

logging.basicConfig(level=logging.INFO)


class NoisyImportFinder:

    PATH_TRIGGER = "__metalpipe__"

    def __init__(self, path_entry):
        self.path_entry = path_entry
        print("Checking {}:".format(path_entry), end=" ")
        if path_entry != self.PATH_TRIGGER:
            print("wrong finder")
            raise ImportError()
        else:
            print("works")
        return

    def find_module(self, fullname, path=None):
        logging.info(fullname)
        if fullname == self.PATH_TRIGGER:
            logging.info("Loading derived class")
            import pdb

            pdb.set_trace()

        return None


if __name__ == "__main__":
    sys.path_hooks.append(NoisyImportFinder)

    for hook in sys.path_hooks:
        print("Path hook: {}".format(hook))

    sys.path.insert(0, NoisyImportFinder.PATH_TRIGGER)

    try:
        print("importing target_module")
        import __metalpipe__.bar
    except Exception as e:
        print("Import failed:", e)

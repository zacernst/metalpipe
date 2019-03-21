import ZODB, ZODB.FileStorage
import BTrees.OOBTree
import transaction

from metalpipe.utils.helpers import package, unpackage


class MetalPipeRecorder:
    def __init__(self, record_to_file=None, pipeline=None):
        if pipeline is None:
            raise Exception("``MetalPipeRecorder`` requires pipeline.")
        self.record_to_file = record_to_file
        self.pipeline = pipeline
        self.record = self.init_record()
        for node in pipeline.all_connected():
            self.record_value(node, "__num_values_stored__", -1)  # record_value will increment this to 0

    def broken_record_storage(self):
        storage = ZODB.FileStorage.FileStorage(self.record_to_file)
        db = ZODB.DB(storage)
        connection = db.open()
        root = connection.root
        root.nodes = BTrees.OOBTree.BTree()
        return root

    def init_record(self):
        root = self.broken_record_storage()
        for node in self.pipeline.all_connected():
            setattr(root, node.name, BTrees.OOBTree.BTree())
        return root

    def num_values_stored(self, node):
        return int(self.get_value(node, '__num_values_stored__'))

    def record_value(self, node, key, value):
        self.node_record(node)[key] = package(value)
        self.node_record(node)["__num_values_stored__"] = package(
            unpackage(self.node_record(node)["__num_values_stored__"]) + 1
        )
        transaction.commit()

    def get_value(self, node, key):
        try:
            value = unpackage(self.node_record(node)[key])
        except:
            value = None
        return value

    def node_record(self, node):
        return getattr(self.record, node.name)

    def delete_value(self, node, key):
        del self.node_record(node)[key]

    def push_value(self, node, key, value):
        current_value = self.get_value(node, key) or []
        current_value.append(value)
        self.record_value(node, key, current_value)

    def pop_value(self, node, key):
        """
        FIFO
        """
        value_list = self.get_value(node, key)
        popped_value = value_list[0]
        value_list = value_list[1:]
        if len(value_list) == 0:
            value_list = None
        self.record_value(node, key, value_list)
        return value_list


if __name__ == "__main__":

    from metalpipe.node import *
    from metalpipe.node_classes.network_nodes import *
    from metalpipe.node_classes.civis_nodes import *

    node_1 = ConstantEmitter(name="node_1", thing={"hi": "there"})
    node_2 = PrinterOfThings(name="node_2")
    node_1 > node_2

    record = MetalPipeRecorder(record_to_file="test_file.fs", pipeline=node_1)
    node_1.global_start(record_to_file="record_1.fs")
    record.record_value(node_1, "hi", "there")
    record.push_value(node_2, "hi", "bob")
    record.push_value(node_2, "hi", "alice")

    print(record.get_value(node_1, "hi"))
    print(record.get_value(node_2, "__num_values_stored__"))

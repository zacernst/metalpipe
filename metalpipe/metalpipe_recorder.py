import ZODB, ZODB.FileStorage
import BTrees.OOBTree
import transaction


class MetalPipeRecorder:

    def __init__(self, filename=None, pipeline=None):
        self.filename = filename
        self.pipeline = pipeline
        self.record = self.init_record()

    def broken_record_storage(self):
        storage = ZODB.FileStorage.FileStorage(self.filename)
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

    def record_value(self, node, key, value):
        self.node_record(node)[key] = value
        transaction.commit()

    def get_value(self, node, key):
        try:
            value = self.node_record(node)[key]
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
        '''
        FIFO
        '''
        value_list = self.get_value(node, key)
        popped_value = value_list[0]
        value_list = value_list[1:]
        if len(value_list) == 0:
            value_list = None
        self.record_value(node, key, value_list)
        return value_list


if __name__ == '__main__':

    from metalpipe.node import *
    from metalpipe.node_classes.network_nodes import *
    from metalpipe.node_classes.civis_nodes import *

    node_1 = ConstantEmitter(name='node_1', thing={'hi': 'there'})
    node_2 = PrinterOfThings(name='node_2')
    node_1 > node_2

    record = MetalPipeRecorder(filename='test_file.fs', pipeline=node_1)
    # node_1.global_start()
    record.record_value(node_1, 'hi','there')
    record.push_value(node_2, 'hi', 'bob')
    record.push_value(node_2, 'hi', 'alice')

    print(record.get_value(node_1, 'hi'))

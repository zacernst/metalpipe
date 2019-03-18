import ZODB, ZODB.FileStorage
import BTrees.OOBTree


def broken_record_storage(filename, pipeline):
    storage = ZODB.FileStorage.FileStorage(filename)
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root
    root.nodes = BTrees.OOBTree.BTree()

    for node in pipeline.all_connected():
        setattr(root, node.name, BTrees.OOBTree.BTree())

    return root


if __name__ == '__main__':

    from metalpipe.node import *
    from metalpipe.node_classes.network_nodes import *
    from metalpipe.node_classes.civis_nodes import *

    node_1 = ConstantEmitter()
    node_2 = PrinterOfThings()
    node_1 > node_2

    storage = broken_record_storage('test_file.fs', node_1)

import hashlib
import redis

from metalpipe.utils.helpers import package, unpackage


class RedisFixturizer:
    def __init__(self, db=0, host="localhost", port=6379):
        self.db = db
        self.host = host
        self.port = port
        self.redis = redis.Redis(host=host, port=port, db=db)

    def get_num_source_messages(self, node):
        node_key = "__num_values_stored_{node_name}__".format(
            node_name=node.name
        )
        try:
            num_messages = self.redis.get(node_key)
        except:
            num_messages = -1
        if num_messages is None:
            num_messages = -1
        return num_messages

    def record_source_node(self, node, value):
        num_messages = int(self.get_num_source_messages(node))
        message_number = num_messages + 1
        key = "{node_name}_{message_number}".format(
            node_name=node.name, message_number=str(message_number)
        )
        self.redis.set(key, package(value))
        node_key = "__num_values_stored_{node_name}__".format(
            node_name=node.name
        )
        self.redis.set(node_key, message_number)

    def record_worker_node(self, node, input_value, output_value):
        input_hash = hashlib.md5(bytes(str(input_value), "utf8")).hexdigest()
        key = "{node_name}_{input_hash}".format(
            node_name=node.name, input_hash=input_hash
        )
        try:
            value = self.redis.get(key)
        except:
            value = None
        value = unpackage(value) if value is not None else []
        value.append(output_value)
        self.redis.set(key, package(value))


if __name__ == "__main__":

    from metalpipe.node import *
    from metalpipe.node_classes.network_nodes import *
    from metalpipe.node_classes.civis_nodes import *

    node_1 = ConstantEmitter(
        name="node_1",
        delay=0.01,
        max_loops=200,
        thing={"hi": "there"},
        fixturize=True,
    )
    node_2 = PrinterOfThings(fixturize=True, name="node_2")
    node_3 = PrinterOfThings(fixturize=True, name="node_3")
    node_1 > node_2 > node_3

    # record = MetalPipeRecorder(record_to_file="test_file.fs", pipeline=node_1)
    node_1.global_start(fixturize=True)
    node_1.wait_for_pipeline_finish()
    sys.exit()

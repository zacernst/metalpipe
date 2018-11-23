import json
from jsonschema import validate
from nanostream.nanostream_processor import (NanoStreamProcessor)


class JsonValidator(NanoStreamProcessor):
    def __init__(self, schema_filename):

        with open(schema_filename, 'r') as json_schema_file:
            self.schema = json.load(json_schema_file)

        super(JsonValidator, self).__init__()

    def process_item(self):
        if isinstance(self.message, str):
            self.message = json.loads(self.message)
        try:
            validate(item, self.schema)
            return self.message
        except ValidationError:
            pass  # We'll log the error here

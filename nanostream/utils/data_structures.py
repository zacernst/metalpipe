'''
Data types (e.g. Rows, Records) for ETL.
'''

from datetime import datetime
import uuid


class DataType:

    @classmethod
    def __repr__(cls):
        return 'hi'


class STRING(DataType):
    pass


class INTEGER(DataType):
    pass


class DATETIME(DataType):
    pass


class FLOAT(DataType):
    pass


for chars in range(128):
    type_name = 'MYSQL_VARCHAR' + str(chars)
    t = type(type_name, (DataType,), {})
    globals()[type_name] = t


for chars in range(32):
    type_name = 'MYSQL_INT' + str(chars)
    t = type(type_name, (DataType,), {})
    globals()[type_name] = t


class MYSQL_DATE(DataType):
    pass


DATA_TYPE_MAPPING = {
    int: INTEGER,
    str: STRING,
    float: FLOAT,
    datetime: DATETIME}


MYSQL_STRING_MAPPING = {
    'date': MYSQL_DATE}


class Record:
    '''
    A single value, with type.
    '''

    def __init__(self, value, data_type, name=None):
        self.value = value
        self.data_type = data_type
        self.name = name or uuid.uuid4().hex

    def __repr__(self):
        out = '[{name}: {value}, {data_type}]'.format(
            name=str(self.name),
            value=str(self.value),
            data_type=str(self.data_type))
        return out


class Row:
    '''
    A collection of ``Record``s
    '''

    def __init__(self, *records):
        self.records = {
            record.name: record for record in records}

    def __getattr__(self, attr):
        if attr not in self.__dict__ and attr in self.records:
            return self.records[attr]
        return super(Row, self).__init__()

    def __repr__(self):
        return ', '.join([str(record) for record in self.records.values()])

    @staticmethod
    def from_dict(row_dictionary):
        record_list = [
            Record(value, STRING, name=key)
            for key, value in row_dictionary.items()]
        return Row(*record_list)

    def concat(self, other, fail_on_duplicate=True):
        if (
            len(set(self.records.keys()) & set(other.records.keys())) > 0 and
                fail_on_duplicate):
            raise Exception(
                'Overlapping records during concatenation of `Row`.')
        self.records.update(other.records)
        return self


if __name__ == '__main__':
    r1 = Record('foo', 'string', name='fooname')
    r2 = Record('bar', 'int', name='barname')
    row = Row(r1, r2)

'''
Data types (e.g. Rows, Records) for ETL.
'''

from datetime import datetime
import uuid

MAX_LENGTH_POWER_OF_TWO = 16
INTEGER_LENGTHS = [2**i for i in range(MAX_LENGTH_POWER_OF_TWO)]
VARCHAR_LENGTHS = [2**i for i in range(MAX_LENGTH_POWER_OF_TWO)]


class IncompatibleTypesException(Exception):
    pass


class DataSourceTypeSystem:
    '''
    Information about mapping one type system onto another contained in
    the children of this class.
    '''
    pass


class PythonTypeSystem(DataSourceTypeSystem):
    pass


class MySQLTypeSystem(DataSourceTypeSystem):
    pass


class TranslationLayerTypeSystem(DataSourceTypeSystem):
    pass


class DataType:

    python_cast_function = (lambda x: x)

    def __init__(self, value, name=None):
        self.value = value
        self.name = name or uuid.uuid4().hex

    @classmethod
    def __repr__(cls):
        return 'hi'

    def __repr__(self):
        return ':'.join([str(self.value), self.__class__.__name__])


class STRING(DataType, IntermediateTypeSystem):
    python_cast_function = str


class INTEGER(DataType, IntermediateTypeSystem):
    python_cast_function = int


class DATETIME(DataType, IntermediateTypeSystem):
    python_cast_function = (lambda x: x)


class FLOAT(DataType, IntermediateTypeSystem):
    python_cast_function = float


class BOOL(DataType, IntermediateTypeSystem):
    python_cast_function = bool


class MYSQL_DATE(DataType, MySQLTypeSystem):
    python_cast_function = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d')


class MYSQL_BOOL(DataType, MySQLTypeSystem):
    pass


###############
# MYSQL TYPES #
###############


class MYSQL_VARCHAR_BASE(DataType, MySQLTypeSystem):
    python_cast_function = str


class MYSQL_ENUM(DataType, MySQLTypeSystem):
    python_cast_function = str  # Placeholder


class MYSQL_VARCHAR(type):
    def __new__(cls, max_length):
        x = super().__new__(
            cls,
            'MYSQL_VARCHAR{max_length}'.format(max_length=str(max_length)),
            (MYSQL_VARCHAR_BASE, ), {'max_length': max_length})
        return x


class MYSQL_INTEGER_BASE(DataType):
    python_cast_function = int


class MYSQL_INTEGER(type):
    def __new__(cls, max_length):
        x = super().__new__(
            cls, 'MYSQL_INT{max_length}'.format(max_length=str(max_length)),
            (MYSQL_INTEGER_BASE, ), {'max_length': max_length})
        return x


for varchar_length in VARCHAR_LENGTHS:
    globals()['MYSQL_VARCHAR' +
              str(varchar_length)] = MYSQL_VARCHAR(varchar_length)

for integer_length in INTEGER_LENGTHS:
    globals()['MYSQL_INTEGER' +
              str(integer_length)] = MYSQL_INTEGER(integer_length)


def mysql_type(string):
    string = string.lower()
    if string.startswith('int') and '(' in string:
        max_length = string[4:-1]
        cls = globals()['MYSQL_INT' + max_length]
    elif string.startswith('int'):
        cls = INTEGER_BASE
    elif string.startswith('varchar') and '(' in string:
        max_length = string[8:-1]
        cls = globals()['MYSQL_VARCHAR' + max_length]
    elif string.startswith('varchar'):
        cls = VARCHAR_BASE
    elif string == 'date':
        cls = MYSQL_DATE
    else:
        raise Exception('Unrecognized MySQL type: {type_string}'.format(
            type_string=string))
    return cls


class Row:
    '''
    A collection of ``DataType`` objects (typed values). They are dictionaries
    mapping the names of the values to the ``DataType`` objects.
    '''

    def __init__(self, *records, type_system=PythonTypeSystem):
        '''
        Constructor for ``Row``.

        Args:
            records: A list of ``DataType`` objects
        '''

        self.records = {record.name: record for record in records}
        self.type_system = type_system

    def is_empty(self):
        return len(self.records) == 0

    def __getattr__(self, attr):
        '''
        Overrides the usual ``__getattr__`` method. The purpose of this
        is to make each ``Record`` in the ``Row`` accessible as an attribute.
        '''

        if attr not in self.__dict__ and attr in self.records:
            return self.records[attr]
        return super(Row, self).__init__()

    def __repr__(self):
        return ', '.join([str(record) for record in self.records.values()])

    @staticmethod
    def from_dict(row_dictionary):
        '''
        Creates a ``Row`` object form a dictionary mapping names to values.
        '''

        record_list = [
            Record(value, STRING, name=key)
            for key, value in row_dictionary.items()
        ]
        return Row(*record_list)

    def concat(self, other, fail_on_duplicate=True):
        if (len(set(self.keys()) & set(other.keys())) > 0
                and fail_on_duplicate):
            raise Exception(
                'Overlapping records during concatenation of `Row`.')
        self.records.update(other.records)
        return self

    def keys(self):
        '''
        For implementing the mapping protocol.
        '''
        return self.records.keys()

    def __getitem__(self, key):
        '''
        Cast to a dictionary and get back Python types.
        '''
        obj = getattr(self, key)
        return obj.to_python()

    def __del__(self, key):
        del self.records[key]

    def __iadd__(self, other):
        if self.type_system is not other.type_system:
            raise IncompatibleTypesException(
                '''Tried to concatenate `Row` objects with incompatible '''
                '''type systems {type_1} and {type_2}.'''.format(
                    type_1=self.type_system.__name__,
                    type_2=other.type_system.__name__))
        concat(self, other, fail_on_duplicate=False)


if __name__ == '__main__':
    foo = MYSQL_VARCHAR16('foo', name='foo')
    bar = MYSQL_INTEGER8(12)
    r = Row(foo, bar, type_system=MySQLTypeSystem)
    t = Row(type_system=PythonTypeSystem)

"""
Data structures module
======================

Data types (e.g. Rows, Records) for ETL.
"""

from datetime import datetime
import uuid

MAX_LENGTH_POWER_OF_TWO = 16
INTEGER_LENGTHS = set(2 ** i for i in range(MAX_LENGTH_POWER_OF_TWO))
INTEGER_LENGTHS = INTEGER_LENGTHS | (set(i for i in range(32)))
VARCHAR_LENGTHS = set(2 ** i for i in range(MAX_LENGTH_POWER_OF_TWO))
VARCHAR_LENGTHS = VARCHAR_LENGTHS | (set(i for i in range(32)))


class IncompatibleTypesException(Exception):
    pass


class DataSourceTypeSystem:
    """
    Information about mapping one type system onto another contained in
    the children of this class.
    """

    @staticmethod
    def convert(obj):
        """
        Override this method if something more complicated is necessary.
        """
        obj = convert_to_type_system(obj, MySQLTypeSystem)
        return obj

    @staticmethod
    def type_mapping(*args, **kwargs):
        raise NotImplemented(
            "Class does not have a ``type_mapping`` function."
        )


def convert_to_type_system(obj, cls):
    members_of_type_system = [
        i
        for i in globals().values()
        if hasattr(i, "__bases__") and cls in all_bases(i)
    ]
    max_length = getattr(obj.original_type, "max_length", None)
    matching_max_length = [
        i
        for i in members_of_type_system
        if getattr(i, "max_length", None) == max_length
    ]
    matching_intermediate_data_type = [
        i for i in matching_max_length if i.intermediate_type is obj.__class__
    ]
    if len(matching_intermediate_data_type) == 0:
        raise Exception("No matching intermediate data type.")
    elif len(matching_intermediate_data_type) > 1:
        raise Exception("More than one matching intermediate data type")
    else:
        pass  # No other cases?
    data_type = matching_intermediate_data_type[0]
    converted_obj = data_type(obj.value)
    if hasattr(obj, "max_length"):
        converted_obj.max_length = obj.max_length
    converted_obj.original_type = obj.original_type
    return converted_obj


class PythonTypeSystem(DataSourceTypeSystem):
    pass


class PrimitiveTypeSystem(DataSourceTypeSystem):
    pass


class MySQLTypeSystem(DataSourceTypeSystem):
    """
    Each ``TypeSystem`` gets a ``type_mapping`` static method that takes a
    string and returns the class in the type system named by that string.
    For example, ``int(8)`` in a MySQL schema should return the
    ``MYSQL_INTEGER8`` class.
    """

    @staticmethod
    def type_mapping(string):
        """
        Parses the schema strings from MySQL and returns the appropriate class.
        """
        string = string.lower()
        if string.startswith("int") and "(" in string:
            max_length = string[4:-1]
            cls = globals()["MYSQL_INTEGER" + max_length]
        elif string.startswith("int"):
            cls = INTEGER_BASE
        elif string.startswith("varchar") and "(" in string:
            max_length = string[8:-1]
            cls = globals()["MYSQL_VARCHAR" + max_length]
        elif string.startswith("varchar"):
            cls = VARCHAR_BASE
        elif string == "date":
            cls = MYSQL_DATE
        else:
            cls = MYSQL_VARCHAR128
            # raise Exception('Unrecognized MySQL type: {type_string}'.format(
            #    type_string=string))
        return cls


class IntermediateTypeSystem(DataSourceTypeSystem):
    """
    Never instantiate this by hand.
    """

    pass


def primitive_to_intermediate_type(thing, name=None):
    if isinstance(thing, (int,)):
        cast_type = INTEGER
    elif isinstance(thing, (float,)):
        cast_type = FLOAT
    elif isinstance(thing, (str,)):
        cast_type = STRING
    else:
        raise Exception("Unknown type")
    return cast_type(thing, name=name, original_type=PrimitiveTypeSystem)


class DataType:
    """
    Each ``DataType`` gets a ``python_cast_function``, which is a function.
    """

    python_cast_function = None
    intermediate_type = None

    def __init__(self, value, original_type=None, name=None):
        self.value = value
        self.name = name or uuid.uuid4().hex
        self.original_type = original_type

    @classmethod
    def __repr__(cls):
        return "hi"

    def __repr__(self):
        return ":".join([str(self.value), self.__class__.__name__])

    def to_intermediate_type(self):
        """
        Convert the ``DataType`` to an ``IntermediateDataType`` using its
        class's ``intermediate_type`` attribute.
        """
        if self.__class__.intermediate_type is None:
            raise Exception("No ``intermediate_type`` cast defined.")
        return self.__class__.intermediate_type(
            self.value, original_type=self.__class__, name=self.name
        )

    def to_python(self):
        if self.__class__.python_cast_function is None:
            raise Exception("No method for casting to Python primitive.")
        else:
            return self.__class__.python_cast_function(self.value)

    @property
    def type_system(self):
        """
        Just for convenience to make the type system an attribute.
        """
        return get_type_system(self)


class STRING(DataType, IntermediateTypeSystem):
    python_cast_function = str


class INTEGER(DataType, IntermediateTypeSystem):
    python_cast_function = int


class DATETIME(DataType, IntermediateTypeSystem):
    python_cast_function = lambda x: x


class FLOAT(DataType, IntermediateTypeSystem):
    python_cast_function = float


class BOOL(DataType, IntermediateTypeSystem):
    python_cast_function = bool


# MYSQL TYPES
#
# Each ``DataType`` has a ``python_cast_function`` and an ``intermediate_type``
# attribute. The ``intermediate_type`` is the class in the
# ``IntermediateTypeSystem`` to which this ``DataType`` would be cast.


class MYSQL_VARCHAR_BASE(DataType, MySQLTypeSystem):
    python_cast_function = str
    intermediate_type = STRING


class MYSQL_ENUM(DataType, MySQLTypeSystem):
    python_cast_function = str  # Placeholder
    intermediate_type = STRING  # Needs to be changed


class MYSQL_DATE(DataType, MySQLTypeSystem):
    python_cast_function = lambda x: datetime.datetime.strptime(x, "%Y-%m-%d")
    intermediate_type = DATETIME


class MYSQL_BOOL(DataType, MySQLTypeSystem):
    python_cast_function = bool
    intermediate_type = BOOL


class MYSQL_VARCHAR(type):
    def __new__(cls, max_length):
        x = super().__new__(
            cls,
            "MYSQL_VARCHAR{max_length}".format(max_length=str(max_length)),
            (MYSQL_VARCHAR_BASE,),
            {"max_length": max_length},
        )
        return x


class MYSQL_INTEGER_BASE(DataType, MySQLTypeSystem):
    python_cast_function = int
    intermediate_type = INTEGER


class MYSQL_INTEGER(type):
    def __new__(cls, max_length):
        x = super().__new__(
            cls,
            "MYSQL_INTEGER{max_length}".format(max_length=str(max_length)),
            (MYSQL_INTEGER_BASE,),
            {"max_length": max_length},
        )
        return x


def make_types():
    types_dict = {}
    for varchar_length in VARCHAR_LENGTHS:
        types_dict["MYSQL_VARCHAR" + str(varchar_length)] = MYSQL_VARCHAR(
            varchar_length
        )

    for integer_length in INTEGER_LENGTHS:
        types_dict["MYSQL_INTEGER" + str(integer_length)] = MYSQL_INTEGER(
            integer_length
        )
    return types_dict


globals().update(make_types())


def mysql_type(string):
    """
    Parses the schema strings from MySQL and returns the appropriate class.
    """
    string = string.lower()
    if string.startswith("int") and "(" in string:
        max_length = string[4:-1]
        try:
            cls = globals()["MYSQL_INTEGER" + max_length]
        except:
            import pdb

            pdb.set_trace()
    elif string.startswith("int"):
        cls = INTEGER_BASE
    elif string.startswith("varchar") and "(" in string:
        max_length = string[8:-1]
        cls = globals()["MYSQL_VARCHAR" + max_length]
    elif string.startswith("varchar"):
        cls = VARCHAR_BASE
    elif string == "date":
        cls = MYSQL_DATE
    else:
        cls = MYSQL_VARCHAR128  # No
        # raise Exception('Unrecognized MySQL type: {type_string}'.format(
        #    type_string=string))
    return cls


class Row:
    """
    A collection of ``DataType`` objects (typed values). They are dictionaries
    mapping the names of the values to the ``DataType`` objects.
    """

    def __init__(self, *records, type_system=None):
        """
        Constructor for ``Row``.

        Args:
            records: A list of ``DataType`` objects
        """

        self.records = {record.name: record for record in records}
        self.type_system = type_system

    def is_empty(self):
        return len(self.records) == 0

    def __getattr__(self, attr):
        """
        Overrides the usual ``__getattr__`` method. The purpose of this
        is to make each ``Record`` in the ``Row`` accessible as an attribute.
        """

        if attr not in self.__dict__ and attr in self.records:
            return self.records[attr]
        return super(Row, self).__getattr__(attr)

    def __repr__(self):
        return ", ".join([str(record) for record in self.records.values()])

    @staticmethod
    def from_dict(row_dictionary, **kwargs):
        """
        Creates a ``Row`` object form a dictionary mapping names to values.
        """

        import pdb

        pdb.set_trace()
        return Row(*record_list)

    def concat(self, other, fail_on_duplicate=True):
        if len(set(self.keys()) & set(other.keys())) > 0 and fail_on_duplicate:
            raise Exception(
                "Overlapping records during concatenation of `Row`."
            )
        self.records.update(other.records)
        return self

    def keys(self):
        """
        For implementing the mapping protocol.
        """
        return self.records.keys()

    def __getitem__(self, key):
        """
        Cast to a dictionary and get back Python types.
        """
        obj = getattr(self, key)
        return obj.to_python()

    def __del__(self, key):
        del self.records[key]

    def __iadd__(self, other):
        if self.type_system is not other.type_system:
            raise IncompatibleTypesException(
                """Tried to concatenate `Row` objects with incompatible """
                """type systems {type_1} and {type_2}.""".format(
                    type_1=self.type_system.__name__,
                    type_2=other.type_system.__name__,
                )
            )
        concat(self, other, fail_on_duplicate=False)


def all_bases(obj):
    """
    Return all the class to which ``obj`` belongs.
    """

    def _inner(thing, bases=None):
        bases = bases or set()
        if not hasattr(thing, "__bases__"):
            thing = thing.__class__
        for i in thing.__bases__ or []:
            bases.add(i)
            bases = bases | _inner(i, bases=bases)
        return bases

    return set(_inner(obj))


def get_type_system(obj):
    bases = all_bases(obj)
    type_system_list = [
        i for i in bases if DataSourceTypeSystem in i.__bases__
    ]
    if len(type_system_list) > 1:
        raise Exception("Belongs to more than one type system?")
    elif len(type_system_list) == 0:
        raise Exception("Belongs to no type system?")
    else:
        return type_system_list[0]


if __name__ == "__main__":
    bar = MYSQL_VARCHAR16("bar", name="bar")
    bar = MYSQL_INTEGER8(12)
    r = Row(bar, bar, type_system=MySQLTypeSystem)
    t = Row(type_system=PythonTypeSystem)
    intermediate = bar.to_intermediate_type()
    converted_bar = MySQLTypeSystem.convert(intermediate)

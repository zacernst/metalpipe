"""
Configuration and model inference engine
========================================

Overview
--------

Classes for representing information about the tables and the mapping from
tables to the data model.

Every fact about the data source is called an ``Assertion``.
There are four types of ``Assertion``:

1. ``PropertyAssertion``: Asserts that a column in a table provides a property of an entity.
2. ``NameAssertion``: Like a ``PropertyAssertion``, but also asserts that the property is
    unique, i.e. that it can be used to uniquely designate that entity.
3. ``RelationshipAssertion``: Asserts that two columns associated with ``NameAssertions``
    provide information about a relationship between two entities.
4. ``CoreferenceAssertion``: Asserts that two columns associated with ``NameAssertion``s
   reference the same entity. For example, "cross-walk" tables would necessarily contain at
   least one ``CoreferenceAssertion``.

The configuration file is in YAML format. It looks like this:

::

    example_table:
      data_type: table
      data_config:
        - name:
            entity_type: Customer
            property_column: customer_id
            property_type: IdentificationNumber
            function: no_op
            alias: Customer
        - ...
        - ...

The important part of the configuration is the ``data_config`` section, which is a list of simple dictionaries.

The top-level key is one of:

1. ``name``
2. ``property``
3. ``relationship``
4. ``coreference``

which correspond to the four classes described above. Underneath each top-level key are some key-value
pairs that are used to instantiate the ``Assertion``.

Model Inference
---------------

The various ``Assertion`` classes are integrated with a logic inference engine based on the declarative
language Datalog. The implementation of Datalog is provided by the package pyDatalog.

The pyDatalog module maintains a knowledge base of facts and definitions. As the configuration file is parsed,
the ``Assertion`` objects are instantiated. Throughout the instantiation process (i.e. in the class's
``__init__`` method), any (what we shall call) "primitive facts" are recorded into the pyDatalog
knowledge base. The reason for doing this is so that pyDatalog can infer any missing facts about
the model. For example, the user might not explicitly define the property a particular column provides;
but if there is only one possible property for the column (based on other facts that have been provided
by the user), the engine will infer the missing fact. The inference engine also provides convenient ways
to test for ambiguities and contradictions within the data model.
"""

import logging
import hashlib
import uuid
import itertools
import pprint
import yaml

from metalpipe.node import MetalNode, NothingToSeeHere

from pyDatalog import pyDatalog, Logic


logging.basicConfig(level=logging.ERROR)


variable_bases = ["X", "Y", "Z", "W", "V", "U"]
variable_range = 10
variable_names = list(
    "".join([x, y])
    for x, y in itertools.product(
        variable_bases, [str(digit) for digit in range(variable_range)]
    )
)

test_row = {
    "customer_id": 10,
    "email": "bob@bob.com",
    "height": "3 feet",
    "referring_customer_id": 11,
}

vocabulary = [
    "table_and_name_column_has_variable",
    "assertion_in_table",
    "assertion_has_relationship_property_column",
    "assertion_has_column",
    "assertion_has_compound_name_component",
    "assertion_without_entity_type",
    "column_has_property_type",
    "contains_assertion",
    "property_of_entity_type",
    "entity_has_property",
    "is_entity_type",
    "is_column",
    "is_property",
    "is_assertion",
    "is_relationship_assertion",
    "is_relationship_property_assertion",
    "is_compound_name_component",
    "is_relationship_property",
    "relationship_property_type",
    "is_property_assertion",
    "is_table_data_source",
    "assertion_includes_name_column",  # For compound name assertions
    "assertion_has_property_column",
    "assertion_has_parent_table",
    "assertion_has_relationship_property_type",
    "assertion_without_column",
    "inferred_entity_type",
    "assertion_has_entity_name_column",
    "has_name",
    "is_name_assertion",
    "is_compound_name_assertion",
    "is_coreference_assertion",
    "assertion_has_entity_type",
    "assertion_has_property_type",
    "compound_name_component_has_column_name",
    "compound_name_component_has_component_type",
    "assertion_has_source_entity_name_column",
    "assertion_has_target_entity_name_column",
    "relationship_has_source_entity_type",
    "relationship_has_target_entity_type",
    "relationship_has_source_name_property",
    "table_has_column",
] + variable_names

pyDatalog.create_terms(",".join(vocabulary))

# No entity type for NameAssertion
# See if a property with a named entity type is using that NameAssertion's column

+is_coreference_assertion("_")

assertion_has_entity_type(X0, X1) <= (
    is_name_assertion(X0) & (X0._entity_type != None) & (X1 == X0._entity_type)
)
assertion_has_entity_type(X0, X1) <= (
    is_property_assertion(X0) & (X0._entity_type != None) & (X1 == X0._entity_type)
)
assertion_has_entity_type(X0, X1) <= (
    is_property_assertion(X0)
    & is_name_assertion(Y0)
    & (Y0._property_column == X0._entity_name_column)
    & assertion_has_entity_type(Y0, X1)
)

# Start inferring things about the model

entity_has_property(X0, X1) <= (
    is_entity_type(X0)
    & is_property(X1)
    & is_property_assertion(Y0)
    & assertion_has_entity_type(Y0, X0)
    & assertion_has_property_type(Y0, X1)
)

+is_relationship_assertion("_")
is_assertion(X0) <= is_name_assertion(X0)
is_assertion(X0) <= is_relationship_property_assertion(X0)
is_assertion(X0) <= is_property_assertion(X0)
is_assertion(X0) <= is_relationship_assertion(X0)
is_assertion(X0) <= is_coreference_assertion(X0)
is_assertion(X0) <= is_compound_name_assertion(X0)

assertion_has_column(X0, X1) <= assertion_has_source_entity_name_column(X0, X1)
assertion_has_column(X0, X1) <= assertion_has_target_entity_name_column(X0, X1)
assertion_has_column(X0, X1) <= assertion_has_entity_name_column(X0, X1)
assertion_has_column(X0, X1) <= assertion_has_property_column(X0, X1)

# infer about tables having columns
table_has_column(X0, X1) <= (
    is_table_data_source(X0)
    & is_assertion(Y0)
    & assertion_in_table(Y0, X0)
    & is_column(X1)
    & assertion_has_column(Y0, X1)
)

assertion_has_parent_table(X0, X1) <= (
    assertion_has_column(X0, Y0) & table_has_column(X1, Y0)
)

assertion_has_property_type(X0, X1) <= (
    assertion_has_property_column(X0, Y0) & column_has_property_type(Y0, X1)
)

# Pierre is not in the cafe
assertion_without_column(X0) <= (
    is_property_assertion(X0) & ~assertion_has_column(X0, Y0)
)
assertion_without_entity_type(X0) <= (
    is_assertion(X0) & ~assertion_has_entity_type(X0, Y0)
)

# relationship source name and target name properties
relationship_has_source_name_property(X0, X1) <= (
    is_relationship_assertion(X0)
    & is_property(X1)
    & is_column(Y0)
    & assertion_has_source_entity_name_column(X0, Y0)
    & is_name_assertion(Y1)
    & assertion_has_property_column(Y1, Y0)
    & column_has_property_type(Y0, X1)
)

logic_engine = Logic(True)


def flatten(nested_thing):
    if not isinstance(nested_thing, (list, tuple, set)):
        yield nested_thing
    else:
        for thing in nested_thing:
            for i in flatten(thing):
                yield i


class AmbiguityException(Exception):
    pass


class YouAreDumbException(Exception):
    pass


def inferred_attribute(f):
    """
    Decorator for ``Assertion`` ``@property`` methods.

    Causes the method to check whether the object has an attribute
    whose name is ``_f``. If it does, and that attribute has a
    non-None value, then return it. If it does not, then call ``f``
    as usual.

    The intended use of this decorator is the following: All attributes
    of ``Assertion`` objects that relate to the data model are recorded
    using names that begin with an underscore "_". But their values are
    accessed through a decorated ``@property`` method without the leading
    underscore. This gives us a chance to dynamically infer the value
    of the attribute if it hasn't been explicitly provided, and we can
    infer it without any ambiguity.

    Thus, these ``@property`` methods first check whether their value
    has been explicitly provided in a configuration file. The code to
    make this check isn't bad, but it's identical for each and every
    such attribute. Thus, this decorator.
    """

    def inner_function(obj):
        if not hasattr(obj, "_" + f.__name__):
            raise YouAreDumbException(
                "You decorated the wrong method or you forgot to define an _attribute."
            )
        if getattr(obj, "_" + f.__name__, None) is not None:
            return getattr(obj, "_" + f.__name__)
        else:
            return f(obj)

    return inner_function


class Assertion(pyDatalog.Mixin):
    """
    In case we find we need some stuff for all assertions to have.
    """

    def __init__(self, **kwargs):  # All kwargs are passed from subclass
        self.uuid = uuid.uuid4()
        self._parent_table = kwargs["parent_table"]
        if self._parent_table is not None:
            +is_table_data_source(self._parent_table)
            +assertion_in_table(self, self._parent_table)
        else:
            raise YouAreDumbException("This should not be the case.")
        super(Assertion, self).__init__()

    def __repr__(self):
        out = self.__class__.__name__
        return out

    def inferred(self, attr):
        if not hasattr(self, attr):
            raise Exception(
                "Tested {attr} inferred, but no such attribute.".format(attr=attr)
            )
        if hasattr(self, attr) and not hasattr(self, "_" + attr):
            raise Exception(
                "Tested {attr} inferred; the attribute exists, "
                "but it not hooked up to the inference enigne.".format(attr=attr)
            )
        if getattr(self, "_" + attr, None) is not None:
            return False
        elif getattr(self, attr, None) is not None:
            return True
        else:
            raise Exception("This should not happen.")

    def explicit(self, attr):
        return not self.inferred(attr)


class TableDataSource(pyDatalog.Mixin):
    """
    A configuration for a specific table.
    """

    def __init__(self, config_dict=None, assertion_list=None, config_file=None):
        super(TableDataSource, self).__init__()
        self.config_dict = config_dict
        self.config_file = config_file
        self.assertion_list = assertion_list or []
        if self.config_file is not None:
            self.load_configuration(self.config_file)

    def __repr__(self):  # Causes a recursion error
        out = """TableDataSource: {config_file}""".format(
            config_file=str(self.config_file)
        )
        return out

    def load_configuration(self, config_file):
        if isinstance(config_file, (str,)):
            with open(config_file, "r") as config_file:
                raw_config_file = config_file.read()
                config_dict = yaml.load(raw_config_file)
            self.config_dict = config_dict
            self.name = top_key(self.config_dict)
            self.data_config = self.config_dict[self.name].get("data_config", None)
        else:
            raise NotImplementedError("Provide a string.")

        # Load configuration of model
        for item in self.data_config:
            assertion_type = top_key(item)
            if assertion_type == "name":
                assertion = NameAssertion(parent_table=self, **item["name"])
            elif assertion_type == "property":
                assertion = PropertyAssertion(parent_table=self, **item["property"])
            elif assertion_type == "relationship":
                assertion = RelationshipAssertion(
                    parent_table=self, **item["relationship"]
                )
            elif assertion_type == "coreference":
                assertion = CoreferenceAssertion(
                    parent_table=self, **item["coreference"]
                )
            elif assertion_type == "relationship_property":
                assertion = RelationshipPropertyAssertion(
                    parent_table=self, **item["relationship_property"]
                )
            elif assertion_type == "compound_name":
                assertion = CompoundNameAssertion(
                    parent_table=self, **item["compound_name"]
                )
            else:
                raise Exception(
                    "Unknown Assertion type: {assertion}".format(
                        assertion=assertion_type
                    )
                )
            self.assertion_list.append(assertion)
            +contains_assertion(self, assertion)
        +is_table_data_source(self)

    def parse_config_dict(self):
        """
        Parse the configuration and start building model information
        for the table.
        """
        if self.config_dict is None:
            raise Exception("Need a configuration for the ``TableDataSource``")
        self.name = self.config_dict.get("name", None)


class CoreferenceAssertion(Assertion):

    merge_schema = """
        MERGE
        (X0: {source_entity_type} {{ {source_entity_property}: {source_entity_property_value} }}),
        (X1: {target_entity_type} {{ {target_entity_property}: {target_entity_property_value} }})
        WITH X0, X1
        fill_this_in_later;
    """

    def __init__(
        self,
        source_entity_alias=None,
        target_entity_alias=None,
        source_column=None,
        target_column=None,
        parent_table=None,
        source_entity_type=None,
        target_entity_type=None,
    ):
        super(CoreferenceAssertion, self).__init__(parent_table=parent_table)
        self._source_entity_alias = source_entity_alias
        self._target_entity_alias = target_entity_alias
        self._source_column = source_column
        self._target_column = target_column
        +is_coreference_assertion(self)


class PropertyAssertion(Assertion):

    merge_schema = (
        """MERGE (X0: {entity_type} {{ {entity_name_property}: $entity_name_value }}) """
        """WITH X0 SET X0.{property_type} = $property_value ;"""
    )

    def __init__(
        self,
        parent_table=None,
        property_column=None,
        function=None,
        entity_alias=None,
        entity_type=None,
        alias=None,
        property_type=None,
        entity_name_property=None,
        entity_name_column=None,
    ):
        super(PropertyAssertion, self).__init__(parent_table=parent_table)
        self.function = function
        self._property_column = property_column
        self._property_type = property_type
        self._entity_type = entity_type
        self._entity_alias = entity_alias
        self._alias = alias
        self._entity_name_property = entity_name_property
        self._entity_name_column = entity_name_column

        +is_property_assertion(self)

        if self._entity_type is not None:
            +is_entity_type(self._entity_type)
        if self._property_type is not None:
            +is_property(self._property_type)
            +assertion_has_property_type(self, self._property_type)
        if self._property_column is not None:
            +is_column(self._property_column)
            +assertion_has_property_column(self, self._property_column)
        if self._entity_name_column is not None:
            +is_column(self._entity_name_column)
            +assertion_has_entity_name_column(self, self._entity_name_column)
        if self._property_column is not None and self._property_type is not None:
            +column_has_property_type(self._property_column, self._property_type)

    def cypher(self, row):
        cypher_query = self.merge_schema.format(
            entity_type=self._entity_type,
            entity_name_property=self._entity_name_property,
            # entity_name_value=row[self._entity_name_column],
            property_type=self._property_type
            # property_value=row[self._property_column],
        )
        output_query = {
            "cypher_query": cypher_query,
            "cypher_query_parameters": {
                "entity_name_value": row[self._entity_name_column],
                "property_value": row[self._property_column],
            },
        }
        return output_query

    @property
    @inferred_attribute
    def entity_type(self):
        """
        Check if self._entity_type is explictly defined. If so, return it.
        If not, call ``assertion_has_entity_type`` on ``self`` and see if
        there is exactly one result. If there is, return it. If there is
        more than one result, then the definitions are screwy; raise an
        ``AmbiguityException``. If there are none, then return ``None``.
        """
        inferred_entity_type_list = assertion_has_entity_type(self, X0)
        if len(inferred_entity_type_list) == 0:
            return None
        elif len(inferred_entity_type_list) > 1:
            raise AmbiguityException()
        else:  # exactly one match
            inferred_entity_type = inferred_entity_type_list[0][0]
            return inferred_entity_type

    @property
    @inferred_attribute
    def parent_table(self):
        # If table_has_column
        table_list = assertion_has_parent_table(self, X0)
        if len(table_list) == 0:
            return None
        elif len(table_list) > 1:
            raise AmbiguityException()
        else:
            inferred_parent_table = table_list[0][0]
            return inferred_parent_table

    @property
    @inferred_attribute
    def property_type(self):
        raise AmbiguityException("Haven't got the logic for ``property_type`` yet")


class NameAssertion(PropertyAssertion):

    merge_schema = (
        """MERGE (X0: {entity_type} {{ {property_type}: $property_value }} );"""
    )

    def __init__(self, **kwargs):
        super(NameAssertion, self).__init__(**kwargs)
        +is_name_assertion(self)

    def cypher(self, row):
        """
        ``row`` is a dictionary where each key is a column name.
        """
        property_value = row[self._property_column]
        cypher_query = self.merge_schema.format(
            entity_type=self._entity_type,
            property_type=self._property_type,
            # property_value=property_value,
        )
        output_query = {
            "cypher_query": cypher_query,
            "cypher_query_parameters": {"property_value": property_value},
        }
        return output_query


class CompoundNameComponent(pyDatalog.Mixin):
    def __init__(
        self,
        component_type=None,
        column_name=None,
        entity_name_property=None,
        entity_type=None,
    ):
        super(CompoundNameComponent, self).__init__()
        self.component_type = component_type
        self.column_name = column_name
        self._entity_type = entity_type
        self._entity_name_property = entity_name_property
        +is_compound_name_component(self)
        +compound_name_component_has_column_name(self, self.column_name)
        +compound_name_component_has_component_type(self, self.component_type)

    def __repr__(self):
        out = f"CompoundNameComponent({self.component_type})"
        return out

    @property
    @inferred_attribute
    def entity_type(self):
        raise AmbiguityException("Haven't got the logic for ``entity_type`` yet")

    @property
    @inferred_attribute
    def entity_name_property(self):
        raise AmbiguityException(
            "Haven't got the logic for ``entity_name_property`` yet"
        )


class CompoundNameAssertion(Assertion):
    """
    When the unit of analysis for a row is more than one column.
    """

    merge_schema = None

    def __init__(self, components=None, entity_type=None, parent_table=None, **kwargs):
        super(CompoundNameAssertion, self).__init__(parent_table=parent_table)
        self.components = components
        self._parent_table = parent_table
        self._entity_type = entity_type
        +assertion_in_table(self, self._parent_table)
        +is_compound_name_assertion(self)
        for compound_name_component in components:
            component = CompoundNameComponent(
                component_type=compound_name_component["type"],
                column_name=compound_name_component["column_name"],
                entity_type=compound_name_component["entity_type"],
                entity_name_property=compound_name_component["entity_name_property"],
            )
            +assertion_has_compound_name_component(self, component)

    @staticmethod
    def component_hash(*components):
        pass

    def cypher(self, row):
        """
        ``row`` is a dictionary where each key is a column name.
        """
        # Get the component name assertions
        counter = 0
        output_value_dict = {}
        cypher_list = []
        for (
            compound_name_assertion,
            table_data_source,
            compound_name_component,
            column_name,
        ) in (
            is_compound_name_assertion(X0)
            & assertion_in_table(X0, X1)
            & assertion_has_compound_name_component(X0, X2)
            & compound_name_component_has_column_name(X2, X3)
        ):
            # Start here -- make consistent with loop in line above
            merge = (
                f"MERGE (X{counter}: {compound_name_component.entity_type} "
                f"{{ {compound_name_component.entity_name_property}: $property_value_{counter} }})"
            )
            with_bridge = "WITH " + ", ".join(
                [f"X{sub_counter}" for sub_counter in range(counter + 1)]
            )
            cypher_list.append(merge)
            cypher_list.append(with_bridge)

            output_value_dict[f"property_value_{counter}"] = row[column_name]
            counter += 1
        cypher = " ".join(cypher_list)

        compound_name_entity_cypher = f"MERGE (Y: {self.entity_type} {{ uuid: $uuid }})"

        cypher_list.append(compound_name_entity_cypher)

        for variable_number in range(len(output_value_dict)):
            cypher_list.append(with_bridge + ", Y")
            cypher_list.append(f"MERGE (X{variable_number})-[:R{variable_number}]->(Y)")

        entity_uuid = uuid.uuid4().hex
        output_value_dict["uuid"] = entity_uuid
        cypher_query = " ".join(cypher_list)
        output_query = {
            "cypher_query": cypher_query,
            "cypher_query_parameters": output_value_dict,
        }
        print(output_query)
        return output_query

    @property
    @inferred_attribute
    def entity_name_columns(self):
        raise AmbiguityException(
            "Haven't got the logic for ``entity_name_columns`` yet"
        )

    @property
    @inferred_attribute
    def entity_type(self):
        raise AmbiguityException("Haven't got the logic for ``entity_type`` yet")


class RelationshipAssertion(Assertion):

    merge_schema = (
        "MERGE (X0: {source_entity_type} {{ {source_name_property}: $source_name_value }}) "
        "WITH X0 "
        "MERGE (X1: {target_entity_type} {{ {target_name_property}: $target_name_value }}) "
        "WITH X0, X1 "
        "MERGE (X0)-[:{relationship_type}]->(X1);"
    )

    def __init__(
        self,
        parent_table=None,
        source_entity_alias=None,
        target_entity_alias=None,
        source_entity_type=None,
        target_entity_type=None,
        alias=None,
        relationship_name=None,
        relationship_type=None,
        source_entity_name_column=None,
        target_entity_name_column=None,
        source_name_property=None,
        target_name_property=None,
    ):
        super(RelationshipAssertion, self).__init__(parent_table=parent_table)
        # self._parent_table = parent_table
        self._source_entity_alias = source_entity_alias
        self._target_entity_alias = target_entity_alias
        self._source_entity_type = source_entity_type
        self._target_entity_type = target_entity_type
        self._source_entity_name_column = source_entity_name_column
        self._target_entity_name_column = target_entity_name_column
        self._alias = alias
        self._source_name_property = source_name_property
        self._target_name_property = target_name_property
        self._relationship_type = relationship_type
        +is_relationship_assertion(self)

        if self._parent_table is not None:
            +is_table_data_source(self._parent_table)
            +assertion_in_table(self, self._parent_table)
        if self._source_entity_type is not None:
            +is_entity_type(self._source_entity_type)
            +relationship_has_source_entity_type(self, self._source_entity_type)
        if self._target_entity_type is not None:
            +is_entity_type(self._target_entity_type)
            +relationship_has_target_entity_type(self, self._target_entity_type)
        if self._source_name_property is not None:
            +is_property(self._source_name_property)
        if self._target_name_property is not None:
            +is_property(self._target_name_property)
        if self._source_entity_name_column is not None:
            +assertion_has_source_entity_name_column(
                self, self._source_entity_name_column
            )
        if self._target_entity_name_column is not None:
            +assertion_has_target_entity_name_column(
                self, self._target_entity_name_column
            )

    @property
    @inferred_attribute
    def source_entity_type(self):
        raise AmbiguityException("Haven't got the logic for ``source_entity_type`` yet")

    @property
    @inferred_attribute
    def target_entity_type(self):
        raise AmbiguityException("Haven't got the logic for ``target_entity_type`` yet")

    @property
    @inferred_attribute
    def source_entity_name_column(self):
        raise AmbiguityException(
            "Haven't got the logic for ``source_entity_name_column`` yet"
        )

    @property
    @inferred_attribute
    def target_entity_name_column(self):
        raise AmbiguityException(
            "Haven't got the logic for ``target_entity_name_column`` yet"
        )

    @property
    @inferred_attribute
    def source_name_property(self):
        witnesses = relationship_has_source_name_property(self, X1)
        if len(witnesses) == 0:
            return None
        elif len(witnesses) > 1:
            raise AmbiguityException()
        else:  # exactly one match
            inferred_source_name_property = witnesses[0][0]
            return inferred_source_name_property

    @property
    @inferred_attribute
    def target_name_property(self):
        raise AmbiguityException(
            "Haven't got the logic for ``target_name_property`` yet"
        )

    def cypher(self, row):
        cypher_query = self.merge_schema.format(
            source_entity_type=self._source_entity_type,
            source_name_property=self._source_name_property,
            # source_name_value=row[self._source_entity_name_column],
            target_entity_type=self._target_entity_type,
            target_name_property=self._target_name_property,
            # target_name_value=row[self._target_entity_name_column],
            relationship_type=self._relationship_type,
        )
        output_query = {
            "cypher_query": cypher_query,
            "cypher_query_parameters": {
                "source_name_value": row[self._source_entity_name_column],
                "target_name_value": row[self._target_entity_name_column],
            },
        }
        return output_query


def top_key(some_dict):
    key_list = list(some_dict.keys())
    if len(key_list) != 1:
        raise Exception(
            "top_key called on dictionary without exactly one top-level key."
        )
    top_key = key_list[0]
    return top_key


class GraphNode(MetalNode):
    """
    The class that routes rows to cypher generators.
    """

    _import_pydatalog = True

    def __init__(self, config_file=None, input_table=None, **kwargs):

        self.table_config = TableDataSource(config_file=config_file)
        self.input_table = input_table
        self.logic_engine = logic_engine
        Logic(self.logic_engine)
        super(GraphNode, self).__init__()

    def process_item(self):
        +is_name_assertion("_")
        +is_property_assertion("_")
        for assertion in (
            is_assertion(X0)
            & assertion_in_table(X0, X1)
            & (X1.name == self.input_table)
        ):
            assertion = assertion[0]
            cypher_query = assertion.cypher(self.__message__)
            yield {"cypher": cypher_query}


class RelationshipPropertyAssertion(Assertion):

    merge_schema = (
        "MERGE (X0: {source_entity_type} {{ {source_entity_name_property}: $source_entity_name_value }}) "
        "WITH X0 "
        "MERGE (X1: {target_entity_type} {{ {target_entity_name_property}: $target_entity_name_value }}) "
        "WITH X0, X1 "
        "MERGE (X0)-[r:{relationship_type}]->(X1) "
        "WITH r "
        "SET r.{relationship_property_type} = $relationship_property_value;"
    )

    def __init__(
        self,
        parent_table=None,
        function=None,
        relationship_property_column=None,
        relationship_property_type=None,
        relationship_alias=None,
        relationship_type=None,
        source_entity_name_column=None,
        source_entity_name_property=None,
        target_entity_name_column=None,
        target_entity_name_property=None,
        source_entity_type=None,
        target_entity_type=None,
        alias=None,
        # TODO ? Not sure if we need to add more
    ):
        super(RelationshipPropertyAssertion, self).__init__(parent_table=parent_table)
        self.function = function
        self._relationship_property_column = relationship_property_column
        self._relationship_property_type = relationship_property_type
        self._relationship_type = relationship_type
        self._relationship_alias = relationship_alias
        self._source_entity_name_column = source_entity_name_column
        self._source_entity_name_property = source_entity_name_property
        self._target_entity_name_column = target_entity_name_column
        self._target_entity_name_property = target_entity_name_property
        self._source_entity_type = source_entity_type
        self._target_entity_type = target_entity_type
        self._alias = alias

        +is_relationship_property_assertion(self)

        if self._parent_table is not None:
            +is_table_data_source(self._parent_table)
            +assertion_in_table(self, self._parent_table)
        if self._source_entity_type is not None:
            +is_entity_type(self._source_entity_type)
        if self._target_entity_type is not None:
            +is_entity_type(self._target_entity_type)
        if self._relationship_property_type is not None:
            +is_relationship_property(self._relationship_property_type)
            +assertion_has_relationship_property_type(
                self, self._relationship_property_type
            )
        if self._relationship_property_column is not None:
            +is_column(self._relationship_property_column)
            +assertion_has_relationship_property_column(
                self, self._relationship_property_column
            )
        if self._relationship_property_column is not None:
            +is_column(self._relationship_property_column)
            +assertion_has_relationship_property_column(
                self, self._relationship_property_column
            )

    def cypher(self, row):
        cypher_query = self.merge_schema.format(
            source_entity_type=self._source_entity_type,
            source_entity_name_property=self._source_entity_name_property,
            target_entity_type=self._target_entity_type,
            target_entity_name_property=self._target_entity_name_property,
            relationship_type=self._relationship_type,
            relationship_property_type=self._relationship_property_type,
        )
        output_query = {
            "cypher_query": cypher_query,
            "cypher_query_parameters": {
                "source_entity_name_value": row[self._source_entity_name_column],
                "target_entity_name_value": row[self._target_entity_name_column],
                "relationship_property_value": row[self._relationship_property_column],
            },
        }
        return output_query

    @property
    @inferred_attribute
    def target_entity_name_property(self):
        raise AmbiguityException(
            "Haven't got the logic for ``target_entity_name_property`` yet"
        )

    @property
    @inferred_attribute
    def target_entity_name_column(self):
        raise AmbiguityException(
            "Haven't got the logic for ``target_entity_name_column`` yet"
        )

    @property
    @inferred_attribute
    def source_entity_name_property(self):
        raise AmbiguityException(
            "Haven't got the logic for ``source_entity_name_property`` yet"
        )

    @property
    @inferred_attribute
    def source_entity_name_column(self):
        raise AmbiguityException(
            "Haven't got the logic for ``source_entity_name_column`` yet"
        )

    @property
    @inferred_attribute
    def relationship_alias(self):
        raise AmbiguityException("Haven't got the logic for ``relationship_alias`` yet")

    @property
    @inferred_attribute
    def relationship_type(self):
        raise AmbiguityException("Haven't got the logic for ``relationship_type`` yet")

    @property
    @inferred_attribute
    def relationship_property_type(self):
        raise AmbiguityException(
            "Haven't got the logic for ``relationship_property_type`` yet"
        )

    @property
    @inferred_attribute
    def relationship_property_columns(self):
        raise AmbiguityException(
            "Haven't got the logic for ``relationship_property_column`` yet"
        )

    @property
    @inferred_attribute
    def parent_table(self):
        raise AmbiguityException("Haven't got the logic for ``parent_table`` yet")


if __name__ == "__main__":
    import pdb

    pdb.set_trace()

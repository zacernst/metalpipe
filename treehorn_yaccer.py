"""
The purpose of this parser is to provide a SQL-ish language for querying nested data
structures.

You could type, for example:

..

   SELECT foo, bar FROM my_obj START AT TOP GO DOWN UNTIL HAS KEY baz AS baz_label...

"""


import uuid
import sys
import importlib
import itertools
import ply.yacc as yacc
from pyDatalog import pyDatalog

from treehorn_tokenizer import tokens
from metalpipe.utils import treehorn as treehorn
import logging

logging.basicConfig(level=logging.INFO)


def p_multi_statement(p):
    """
    multi_statement : statement SEMICOLON
                    | multi_statement statement SEMICOLON
    """
    if len(p) == 3:
        p[0] = [p[1]]
    elif len(p) == 4:
        p[0] = p[1]
        p[1].append(p[2])
    else:
        raise Excepttion("This ought not happen.")


# We'll keep these possible root definitions for testing purposes.
def p_root(p):
    """statement : condition
                 | selection_list
                 | traversal
                 | select_clause
                 | property_assertion
                 | relationship_assertion
                 | select_head
                 | function_application
                 | coreference_assertion
                 | function_definition
                 | query_definition
    """

    p[0] = p[1]
    # The split_label method has set the self.label and self.keypath


def p_traversal(p):
    """traversal : START AT condition
                 | GO DOWN UNTIL condition
                 | GO UP UNTIL condition
                 | traversal AS LABEL
    """
    if len(p) == 4 and p[1] == "START" and p[2] == "AT":
        p[0] = treehorn.GoDown(condition=p[3])
    elif len(p) == 5 and p[1] == "GO" and p[2] == "DOWN" and p[3] == "UNTIL":
        p[0] = treehorn.GoDown(condition=p[4])
    elif len(p) == 5 and p[1] == "GO" and p[2] == "UP" and p[3] == "UNTIL":
        p[0] = treehorn.GoUp(condition=p[4])
    elif len(p) == 4 and p[2] == "AS":
        p[0] = p[1]
        (p[0] + p[3])  ###### [p[3]]
    elif len(p) == 2:
        p[0] = p[1]
    else:
        raise Exception("This should never happen.")


class TraversalChain:
    def __init__(self, chain=None):
        self.chain = chain

    def add_step(self, other):
        self.chain.append(other)

    def __repr__(self):
        out = ""
        for index, traversal in enumerate(self.chain):
            out += str(traversal)
            if index < len(self.chain) - 1:
                out += " --> "
        return out


def p_traversal_chain(p):
    """traversal_chain : traversal
                       | traversal_chain traversal
    """
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 3:
        p[0] = p[1]
        p[0].tail > p[2]
    else:
        raise Exception("What?")


def p_condition(p):
    """condition : LPAREN condition AND condition RPAREN
                 | LPAREN condition OR condition RPAREN
                 | NOT condition
                 | TOP
                 | HAS KEY LABEL
    """
    if len(p) == 2 and p[1] == "TOP":
        p[0] = treehorn.IsRoot()
    elif len(p) == 6 and p[3] == "AND":
        p[0] = p[2] & p[4]
    elif len(p) == 6 and p[3] == "OR":
        p[0] = p[2] | p[4]
    elif len(p) == 3 and p[1] == "NOT":
        p[0] = ~p[2]
    elif len(p) == 4 and p[1] == "HAS" and p[2] == "KEY":
        p[0] = treehorn.HasKey(key=p[3])
    else:
        raise Exception("This should not happen.")


def p_keypath(p):
    """ keypath : LABEL
                | keypath DOT LABEL
    """
    if len(p) == 2:
        p[0] = treehorn.KeyPath(traversal_label=p[1])
    elif len(p) == 4:
        p[0] = p[1]
        p[0].keypath.append(p[3])
    else:
        raise Exception("This should definitely not happen.")


def p_selection_list(p):
    """ selection_list : keypath_as
                       | selection_list COMMA keypath_as
    """
    if len(p) == 2:
        p[0] = {
            p[1].label: {
                "keypath": p[1].keypath,
                "traversal_label": p[1].traversal_label,
            }
        }
    elif len(p) == 4:
        p[0] = p[1]
        p[0][p[3].label] = {
            "keypath": p[3].keypath,
            "traversal_label": p[3].traversal_label,
        }
    else:
        raise Exception("What?")


class SelectHead(pyDatalog.Mixin):
    """
    Represents the head of a SELECT clause (e.g. SELECT LABEL, LABEL FROM thing)
    """

    def __init__(self, selection_list=None, obj_name=None):
        self.selection_list = selection_list
        self.obj_name = obj_name
        super(SelectHead, self).__init__()

    def __repr__(self):
        out = "Selecting: {selection_list} from {obj_name}".format(
            selection_list=str(self.selection_list), obj_name=str(self.obj_name)
        )
        return out


def p_select_head(p):
    """ select_head : SELECT selection_list FROM LABEL
    """
    p[0] = SelectHead(selection_list=p[2], obj_name=p[4])


class SelectClause(pyDatalog.Mixin):
    def __init__(self, select_head, traversal_chain):
        self.select_head = select_head
        self.traversal_chain = traversal_chain

    def __repr__(self):
        out = "SelectClause:\n{selections_list}\n{traversal_chain}".format(
            selections_list=str(self.select_head),
            traversal_chain=str(self.traversal_chain),
        )
        return out


def p_select_clause(p):
    """ select_clause : select_head traversal_chain"""
    p[0] = SelectClause(select_head=p[1], traversal_chain=p[2])


def p_query_definition(p):
    """ query_definition : QUERY LABEL IS select_clause"""
    query_name = p[2]
    query = p[4]
    query.name = query_name
    p[0] = query


class Query:
    def __init__(self, query_text, traversal_dict=None, name=None):
        self.query_text = query_text
        self.name = name or uuid.uuid4().hex
        self.query_obj_list = parser.parse(query_text)
        self.query_name = name
        self.traversal_dict = traversal_dict or {}

        # Initialize each query in the query_obj_list based on its class

        for query_obj in self.query_obj_list:
            if isinstance(query_obj, (SelectClause,)):
                traversal_dict = {
                    traversal.label: traversal
                    for traversal in query_obj.traversal_chain.all_traversals()
                    if traversal.label is not None
                }

        # for (
        #    traversal_name,
        #    query_dict,
        # ) in self.query_obj.select_head.selection_list.items():
        #    traversal_dict[
        #        query_dict["traversal_label"]
        #    ].update_retrieval_dict(
        #        key=traversal_name, value=query_dict["keypath"]
        #    )
        # self.relation.traversal = self.query_obj.traversal_chain

    def __repr__(self):
        return "\n".join([str(query_obj) for query_obj in self.query_obj_list])


def p_query_reference(p):
    """
    query_reference : IN QUERY LABEL
    """
    p[0] = p[3]


def p_property_assertion(p):
    """
    property_assertion : query_reference LABEL IS A PROPERTY LABEL OF ENTITY LABEL NAMED BY LABEL
                       | query_reference LABEL IS A UNIQUE PROPERTY LABEL OF ENTITY LABEL
    """
    if len(p) == 13:  # Not unique
        query_name = p[1]
        property_selection_name = p[2]
        property_name = p[6]
        entity_type = p[9]
        entity_selection_name = p[12]
        unique = False
    elif len(p) == 11:  # Unique
        query_name = p[1]
        property_selection_name = p[2]
        entity_selection_name = p[2]
        property_name = p[7]
        entity_type = p[10]
        unique = True
    else:
        raise Exception("This should not happen.")
    p[0] = PropertyAssertion(
        property_name=property_name,
        unique=unique,
        query_name=query_name,
        property_selection_name=property_selection_name,
        entity_selection_name=entity_selection_name,
        entity_type=entity_type,
    )


class init_datalog_obj:
    def __init__(self, foo="bar"):
        self.foo = foo

    def __call__(self, f):
        def inner_function(_self, *args, **kwargs):
            if hasattr(_self, "name"):
                +HAS_NAME(_self, _self.name)
            f(_self, *args, **kwargs)

        return inner_function


class PropertyAssertion(pyDatalog.Mixin):
    def __init__(
        self,
        property_name=None,
        unique=False,
        query_name=None,
        property_selection_name=None,
        entity_selection_name=None,
        entity_type=None,
    ):
        self.property_name = property_name
        self.query_name = query_name
        self.property_selection_name = property_selection_name
        self.entity_type = entity_type
        self.unique = unique
        self.entity_selection_name = entity_selection_name
        super(PropertyAssertion, self).__init__()


def p_coreference_assertion(p):
    """
    coreference_assertion : query_reference LABEL AND LABEL COREFER
    """
    property_name_1 = p[2]
    property_name_2 = p[4]
    query_name = p[1]
    p[0] = CoreferenceAssertion(
        property_name_1=property_name_1,
        property_name_2=property_name_2,
        query_name=query_name,
    )


class CoreferenceAssertion:
    def __init__(self, property_name_1=None, property_name_2=None, query_name=None):
        self.property_name_1 = property_name_1
        self.property_name_2 = property_name_2
        self.query_name = query_name


def p_relationship_assertion(p):
    """
    relationship_assertion : query_reference LABEL NAMED BY LABEL IS RELATED TO LABEL NAMED BY LABEL AS LABEL
    """
    query_name = p[1]
    entity_type_1 = p[2]
    property_name_1 = p[5]
    entity_type_2 = p[9]
    property_name_2 = p[12]
    relationship_name = p[14]
    p[0] = RelationshipAssertion(
        query_name=query_name,
        entity_type_1=entity_type_1,
        property_name_1=property_name_1,
        entity_type_2=entity_type_2,
        property_name_2=property_name_2,
        relationship_name=relationship_name,
    )


def p_pathname(p):
    """
    pathname : LABEL
             | pathname DOT LABEL
    """
    if len(p) == 2:
        p[0] = [p[1]]
    elif len(p) > 2:
        p[0] = p[1]
        p[0].append(p[3])
    else:
        raise Exception("What?")


class RelationshipAssertion(pyDatalog.Mixin):
    def __init__(
        self,
        property_name_1=None,
        entity_type_1=None,
        entity_type_2=None,
        property_name_2=None,
        query_name=None,
        relationship_name=None,
    ):
        self.property_name_1 = property_name_1
        self.property_name_2 = property_name_2
        self.query_name = query_name
        self.entity_type_1 = entity_type_1
        self.entity_type_2 = entity_type_2
        self.relationship_name = relationship_name


def p_function_definition(p):
    """
    function_definition : LABEL IS A PYTHON FUNCTION IMPORTED FROM pathname
    """
    if len(p) == 9 and p[4] == "PYTHON":
        # We're importing a Python function
        p[0] = PythonFunction(name=p[1], pathname=p[8])
    else:
        raise Exception("This should never ever happen under any circumstances.")


class UserDefinedFunction(pyDatalog.Mixin):
    @init_datalog_obj()
    def __init__(self, name=None, pathname=None):
        self.name = name
        self.pathname = pathname or []
        self.function = None
        super(UserDefinedFunction, self).__init__()

        # self.load_function()

    def load_function(self):
        raise Exception(
            "``load_function`` must be defined in child class of ``UserDefinedFunction``."
        )


class PythonFunction(UserDefinedFunction):
    def load_function(self):
        self.function = importlib.import_module(self.pathname[0])
        for function in self.pathname[1:]:
            self.function = getattr(self.function, function)

    def __call__(self, *myargs):
        print('in __call__')
        myargs = tuple(i if not isinstance(i, (treehorn.TracedPrimitive,)) else i.thing for i in myargs)
        return self.function(*myargs)


class FunctionArguments:
    def __init__(self, arg_list=None):
        self.arg_list = arg_list or []

    def __repr__(self):
        out = ":".join([str(i) for i in self.arg_list])
        return out


def p_function_arguments(p):
    """
    function_arguments : keypath
                       | function_application
                       | function_arguments COMMA function_arguments
    """
    if len(p) == 2:
        p[0] = FunctionArguments(arg_list=[p[1]])
    elif len(p) == 4:
        p[0] = p[1]
        p[0].arg_list += p[3].arg_list
    else:
        raise Exception("No")


class FunctionApplication:
    def __init__(self, label=None, function_name=None, function_arguments=None):
        self.function_name = function_name
        self.function_arguments = function_arguments
        self.label = label

    def __repr__(self):
        out = "{function_name}[{function_arguments}]".format(
            function_name=self.function_name,
            function_arguments=str(self.function_arguments),
        )
        return out


def p_function_application(p):
    """
    function_application : LABEL LPAREN function_arguments RPAREN
    """
    p[0] = FunctionApplication(function_name=p[1], function_arguments=p[3])


class SelectionList:
    def __init__(self, label=None, selection_list=None):
        self.selection_list = selection_list or []
        self.label = label


def p_selection_list(p):
    # TODO: Check last rule of this grammar. Not sure if cases in yacc handle it.
    # womp womp.
    """
    selection_list : function_application AS LABEL
                   | keypath AS LABEL
                   | selection_list COMMA function_application AS LABEL
                   | selection_list COMMA keypath AS LABEL
    """
    if len(p) == 4 and isinstance(p[1], (treehorn.KeyPath,)):
        p[0] = SelectionList(
            selection_list=[
                FunctionApplication(
                    function_name="identity",
                    function_arguments=FunctionArguments(arg_list=[p[1]]),
                    label=p[3],
                )
            ]
        )
    elif len(p) == 4:
        function_application = p[1]
        function_application.label = p[3]
        p[0] = SelectionList(selection_list=[function_application])
    elif len(p) == 6 and isinstance(p[3], (treehorn.KeyPath,)):
        function_application = FunctionApplication(
            function_name="identity",
            function_arguments=FunctionArguments(arg_list=[p[3]]),
            label=p[5],
        )
        p[0] = p[1]
        p[0].selection_list.append(function_application)
    elif len(p) == 6 and not isinstance(p[3], (treehorn.KeyPath,)):
        function_application = p[3]
        function_application.label = p[5]
        p[0] = p[1]
        p[0].selection_list.append(function_application)
    else:
        raise Exception("?!")


parser = yacc.yacc()


def evaluate_selection_function(selection_task, one_traversal_result, function_dict):
    if isinstance(selection_task, (treehorn.KeyPath,)):
        full_path = [selection_task.traversal_label] + selection_task.keypath
        obj = one_traversal_result
        for step in full_path:
            obj = obj[step]
        out = obj
    elif isinstance(selection_task, (FunctionApplication,)):
        args = [
            evaluate_selection_function(argument, one_traversal_result, function_dict)
            for argument in selection_task.function_arguments.arg_list
        ]
        print('in evaluate_selection_function')
        out = function_dict[selection_task.function_name](*args)
    else:
        raise Exception()
    return out


class Entity(pyDatalog.Mixin):
    pass


class DataSource(pyDatalog.Mixin):
    def __init__(self, name=None):
        self.name = name or uuid.uuid4().hex
        super(DataSource, self).__init__()


def _de_trace(obj):
    '''
    Remove the ``TracedPrimitive`` wrapper from the object, if necessary.
    '''
    return obj if not isinstance(obj, (treehorn.TracedPrimitive,)) else obj.thing


class UniquePropertyInsertion:
    def __init__(self, entity_type=None, property_type=None, property_value=None):
        self.entity_type = _de_trace(entity_type)
        self.property_type = _de_trace(property_type)
        self.property_value = _de_trace(property_value)

    def to_cypher(self, transaction):
        insertion_string = """MERGE (x:{entity_type} {{ {property_type}: $property_value }});""".format(
            entity_type=self.entity_type,
            property_type=self.property_type,
            # property_value=self.property_value,
        )
        transaction.run(insertion_string, property_value=self.property_value)
        return insertion_string


def load_query_text_to_logic(query_text):
    identity = PythonFunction(name="identity")

    def hi(*args):
        return str(args[0]).upper()

    identity.function = hi

    function_dict = {"identity": identity}
    unique_property_dict = {}
    parsed_query = parser.parse(query_text)
    for query_obj in parsed_query:
        if isinstance(query_obj, (PythonFunction,)):
            +FUNCTION(query_obj)
            +HAS_NAME(query_obj, query_obj.name)
            query_obj.load_function()
        elif isinstance(query_obj, (PropertyAssertion,)):
            +PROPERTY(query_obj)
            +HAS_NAME(query_obj, query_obj.property_name)
            if query_obj.unique:
                +UNIQUE(query_obj)
            +ENTITY(query_obj.entity_type)
            +PROPERTY_OF(query_obj, query_obj.entity_type)
            +REFERENCES_QUERY_NAME(query_obj, query_obj.query_name)
        elif isinstance(query_obj, (SelectClause,)):
            +SELECT_CLAUSE(query_obj)
            +HAS_NAME(query_obj, query_obj.name)
            +DATA_SOURCE(query_obj.select_head.obj_name)
            +DATA_SOURCE_IN_SELECT_CLAUSE(query_obj.select_head.obj_name, query_obj)
            for (
                selection
            ) in (
                query_obj.select_head.selection_list.selection_list
            ):  # Redundant keypath?
                +SELECTION(selection)
                +QUERY_CONTAINS_SELECTION(query_obj, selection)
                # Need to link selection to properties, especially unique properties
                +QUERY_HAS_SELECTION_NAME(query_obj, selection.label)
        elif isinstance(query_obj, (RelationshipAssertion,)):
            +RELATIONSHIP(query_obj)
            +HAS_NAME(query_obj, query_obj.relationship_name)
        else:
            pass

    return function_dict, unique_property_dict


if __name__ == "__main__":
    import json
    import pprint

    from logic import *
    obj = json.load(open("./tests/sample_data/sample_treehorn_1.json"))

    # Order to evaluate query statements:
    # 1. Function assertions
    # 2. Entity unique property assertions
    # 3. Entity non-unique property assertions
    # 4. Relationship assertions
    # 5. Traversals

    with open("./query_text.mtl", "r") as f:
        q = f.read()

    function_dict, unique_property_dict = load_query_text_to_logic(q)


    from neo4j import GraphDatabase

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "imadfs1"))
    session = driver.session()
    transaction = session.begin_transaction()

    for select_clause in SELECT_CLAUSE(X):
        select_clause = select_clause[0]
        query_name = select_clause.name
        for one_traversal_result in select_clause.traversal_chain(obj):
            all_selections_dict = {}
            print("----")
            for (
                selection
            ) in select_clause.select_head.selection_list.selection_list:
                answer = evaluate_selection_function(
                    selection, one_traversal_result, function_dict
                )
                print("answer: ", selection.label, answer)
                all_selections_dict[selection.label] = answer

                # Find all the unique property instances in this set of answers
                unique_property_instances = (
                    PROPERTY_IN_SELECT_CLAUSE(X0, select_clause)
                    & UNIQUE_PROPERTY_OF(X0, X1)
                    & HAS_NAME(X0, X2)
                    & (X0.property_selection_name == selection.label)
                )
                for (
                    property_assertion,
                    entity_type,
                    unique_property_name,
                ) in unique_property_instances:
                    d = UniquePropertyInsertion(
                        entity_type=entity_type,
                        property_type=unique_property_name,
                        property_value=answer,
                    )
                    d.to_cypher(transaction)

            print(all_selections_dict)

            for relationship in QUERY_HAS_RELATIONSHIP(select_clause, X0):
                relationship = relationship[0]
                source_property, target_property = RELATIONSHIP_SOURCE_TARGET_PROPERTIES(
                    relationship, X1, X2
                )[0]
                cypher = (
                    "MERGE (e1: {entity_type_1} {{ {property_name_1}: $property_value_1 }}) WITH e1 "
                    "MERGE (e2: {entity_type_2} {{ {property_name_2}: $property_value_2 }}) WITH e1, e2 "
                    "MERGE (e1)-[:{relationship_type}]->(e2);"
                ).format(
                    entity_type_1=relationship.entity_type_1,
                    property_name_1=source_property,
                    entity_type_2=relationship.entity_type_2,
                    property_name_2=target_property,
                    relationship_type=relationship.relationship_name,
                )
                print('>>>>>', cypher, all_selections_dict[relationship.property_name_1])

                tmp = all_selections_dict[relationship.property_name_1]
                transaction.run(
                    cypher,
                    property_value_1=all_selections_dict[
                        relationship.property_name_1
                    ],
                    property_value_2=all_selections_dict[
                        relationship.property_name_2
                    ],
                )

            # Find non-unique property_instances in this set of answers
            non_unique_property_instances = PROPERTY_IN_SELECT_CLAUSE(
                X0, select_clause
            ) & NON_UNIQUE_PROPERTY_OF(X0, X1)
            for non_unique_property_instance in non_unique_property_instances:

                non_unique_property_instance = non_unique_property_instance[0]
                property_name = non_unique_property_instance.property_name
                query_name = non_unique_property_instance.query_name
                property_selection_name = (
                    non_unique_property_instance.property_selection_name
                )
                entity_type = non_unique_property_instance.entity_type
                entity_selection_name = (
                    non_unique_property_instance.entity_selection_name
                )
                cypher_query = (
                        """MERGE (e: {entity_type} {{ {entity_selection_name}: $entity_selection_value }}) """
                        """WITH e SET e.{property_selection_name} = $property_selection_value;"""
                )
                cypher_query = cypher_query.format(
                    entity_type=entity_type,
                    entity_selection_name=entity_selection_name,
                    property_selection_name=property_name,
                )
                logging.debug(
                    (
                        "property: {property_name},"
                        "property_selection_name: {property_selection_name}, "
                        "entity_type: {entity_type}, "
                        "entity_selection_name: {entity_selection_name}"
                    ).format(
                        property_name=property_name,
                        property_selection_name=property_selection_name,
                        entity_type=entity_type,
                        entity_selection_name=entity_selection_name,
                    )
                )
                print(cypher_query)
                transaction.run(
                    cypher_query,
                    entity_selection_value=all_selections_dict[
                        entity_selection_name
                    ],
                    property_selection_value=all_selections_dict[
                        property_selection_name
                    ],
                )
                # session.run(cypher_query, entity_selection_value=entity_selection_value)
    transaction.commit()



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
import ply.yacc as yacc

from treehorn_tokenizer import tokens
from metalpipe.utils import treehorn as treehorn



def p_multi_statement(p):
    '''
    multi_statement : statement SEMICOLON
                    | multi_statement statement SEMICOLON
    '''
    if len(p) == 3:
        p[0] = [p[1]]
    elif len(p) == 4:
        p[0] = p[1]
        p[1].append(p[2])
    else:
        raise Excepttion('This ought not happen.')


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


class SelectHead:
    """
    Represents the head of a SELECT clause (e.g. SELECT LABEL, LABEL FROM thing)
    """

    def __init__(self, selection_list=None, obj_name=None):
        self.selection_list = selection_list
        self.obj_name = obj_name

    def __repr__(self):
        out = "Selecting: {selection_list} from {obj_name}".format(
            selection_list=str(self.selection_list), obj_name=str(self.obj_name)
        )
        return out


def p_select_head(p):
    """ select_head : SELECT selection_list FROM LABEL
    """
    p[0] = SelectHead(selection_list=p[2], obj_name=p[4])


class SelectClause:
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
        return str(self.query_obj)


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


class PropertyAssertion:
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


class RelationshipAssertion:
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


def p_function_definition(p):
    """
    function_definition : LABEL IS A PYTHON FUNCTION IMPORTED FROM pathname
    """
    if len(p) == 9 and p[4] == "PYTHON":
        # We're importing a Python function
        p[0] = PythonFunction(name=p[1], pathname=p[8])
    else:
        raise Exception("This should never ever happen under any circumstances.")


class UserDefinedFunction:
    def __init__(self, name=None, pathname=None):
        self.name = name
        self.pathname = pathname or []
        self.function = None

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

    def __call__(self, *args):
        return self.function(*args)


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
    """
    selection_list : function_application AS LABEL
                   | selection_list COMMA function_application AS LABEL
    """
    if len(p) == 4:
        function_application = p[1]
        function_application.label = p[3]
        p[0] = SelectionList(selection_list=[function_application])
    elif len(p) == 6:
        function_application = p[3]
        function_application.label = p[5]
        p[0] = p[1]
        p[0].selection_list.append(function_application)
    else:
        raise Exception("?!")


parser = yacc.yacc()


def evaluate_selection_function(selection_task, one_traversal_result):
    if isinstance(selection_task, (treehorn.KeyPath,)):
        # print(selection_task.keypath, selection_task.traversal_label)
        full_path = [selection_task.traversal_label] + selection_task.keypath
        obj = one_traversal_result
        for step in full_path:
            obj = obj[step]
        out = obj
    elif isinstance(selection_task, (FunctionApplication,)):
        args = [
            evaluate_selection_function(argument, one_traversal_result)
            for argument in selection_task.function_arguments.arg_list
        ]
        out = function_dict[selection_task.function_name](*args)
    else:
        raise Exception()
    return out


if __name__ == "__main__":
    import json
    import pprint

    obj = json.load(open("./tests/sample_data/sample_treehorn_1.json"))



    identity = PythonFunction(name="identity")

    def hi(*args):
        return str(args[0]).upper()

    identity.function = hi

    query = Query(
        "QUERY myquery IS "
        "SELECT identity(identity(emaildict.email)) AS emailaddress, "
        "identity(emaildict.username) AS name, identity(address.city) AS cityname "
        "FROM obj START AT TOP GO DOWN UNTIL HAS KEY email AS emaildict "
        "GO DOWN UNTIL HAS KEY city AS address;"
    )

    function_dict = {"identity": identity}




    #for one_traversal_result in query.relation.traversal(obj):
    for one_traversal_result in query.query_obj_list[0].traversal_chain(obj):

        print('----')
        print(one_traversal_result)
        for task in query.query_obj_list:
            if not isinstance(task, (SelectClause,)):
                continue
            for selection in task.select_head.selection_list.selection_list:
                answer = evaluate_selection_function(selection, one_traversal_result)
                print("answer: ", selection.label, answer)


    class Session:
        def __init__(self, function_dict=None):
            self.function_dict = function_dict or {}

        def parse(self, tql):
            result = parser.parse(tql)


    def bak():
        email_address_assertion = (
            """IN QUERY query emailaddress IS A UNIQUE PROPERTY email OF ENTITY person"""
        )
        result_1 = parser.parse(email_address_assertion)
        coreference_assertion = """IN QUERY query name AND emailaddress COREFER"""
        result_2 = parser.parse(coreference_assertion)
        udf_assertion = (
            """foo IS A PYTHON FUNCTION IMPORTED FROM metalpipe.utils.helpers.iterate"""
        )
        result_3 = parser.parse(udf_assertion)
        function_arguments = """myfunction(foo, anotherfunction(bar, baz))"""
        result_4 = parser.parse(function_arguments)

        """LABEL IS A PYTHON FUNCTION IMPORTED FROM pathname"""

        """IN QUERY query cityname IS A UNIQUE PROPERTY OF ENTITY City"""
        """IN QUERY query Person NAMED BY emailaddress IS RELATED TO City NAMED BY cityname AS LivesIn"""
        """IN QUERY query cityname IS A PROPERTY OF ENTITY person NAMED BY emailaddress"""

        selection_list_assertion = """SELECT foo(bar) AS baz FROM whatever"""
        function_application_1 = """foo(bar)"""
        result_1 = parser.parse(function_application_1)
        function_application_2 = """foo(bar.baz)"""
        result_2 = parser.parse(function_application_2)
        function_arguments_1 = """foo(bar) AS whatever"""
        result_3 = parser.parse(function_arguments_1)
        function_arguments_2 = """foo(bar.baz) AS whatever"""
        result_4 = parser.parse(function_arguments_2)
        selection_1 = """foo(bar.baz) AS whatever, goo(goober) AS whateverelse"""
        result_5 = parser.parse(selection_1)
        selection_2 = """foo(bar.baz, qux.buzz) AS whatever, goo(goober) AS whateverelse"""
        result_6 = parser.parse(selection_2)
        selection_head = """SELECT foo(bar.baz, qux.buzz) AS whatever, goo(goober) AS whateverelse FROM thing"""
        result_7 = parser.parse(selection_head)

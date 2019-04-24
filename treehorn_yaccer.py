'''
The purpose of this parser is to provide a SQL-ish language for querying nested data
structures.

You could type, for example:

..

   SELECT foo, bar FROM my_obj START AT TOP GO DOWN UNTIL HAS KEY baz AS baz_label...

'''

import uuid
import ply.yacc as yacc

from treehorn_parser import tokens
from metalpipe.utils import treehorn as treehorn


def p_root(p):
    '''root : condition
            | label_list
            | traversal
            | select_clause'''
    p[0] = p[1]
    # The split_label method has set the self.label and self.keypath


def p_traversal(p):
    '''traversal : START AT condition
                 | GO DOWN UNTIL condition
                 | GO UP UNTIL condition
                 | traversal AS LABEL
    '''
    if len(p) == 4 and p[1] == 'START' and p[2] == 'AT':
        p[0] = treehorn.GoDown(condition=p[3])
    elif len(p) == 5 and p[1] == 'GO' and p[2] == 'DOWN' and p[3] == 'UNTIL':
        p[0] = treehorn.GoDown(condition=p[4])
    elif len(p) == 5 and p[1] == 'GO' and p[2] == 'UP' and p[3] == 'UNTIL':
        p[0] = treehorn.GoUp(condition=p[4])
    elif len(p) == 4 and p[2] == 'AS':
        p[0] = p[1]
        (p[0] + p[3])###### [p[3]]
    elif len(p) == 2:
        p[0] = p[1]
    else:
        raise Exception('This should never happen.')


class TraversalChain:
    def __init__(self, chain=None):
        self.chain = chain

    def add_step(self, other):
        self.chain.append(other)

    def __repr__(self):
        out = ''
        for index, traversal in enumerate(self.chain):
            out += str(traversal)
            if index < len(self.chain) - 1:
                out += ' --> '
        return out


def p_traversal_chain(p):
    '''traversal_chain : traversal
                       | traversal_chain traversal
    '''
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 3:
        p[0] = p[1]
        p[0].tail > p[2]
    else:
        raise Exception('What?')


def p_condition(p):
    '''condition : LPAREN condition AND condition RPAREN
                 | LPAREN condition OR condition RPAREN
                 | NOT condition
                 | TOP
                 | HAS KEY LABEL
    '''
    if len(p) == 2 and p[1] == 'TOP':
        p[0] = treehorn.IsRoot()
    elif len(p) == 6 and p[3] == 'AND':
        p[0] = p[2] & p[4]
    elif len(p) == 6 and p[3] == 'OR':
        p[0] = p[2] | p[4]
    elif len(p) == 3 and p[1] == 'NOT':
        p[0] = ~ p[2]
    elif len(p) == 4 and p[1] == 'HAS' and p[2] == 'KEY':
        p[0] = treehorn.HasKey(key=p[3])
    else:
        raise Exception('This should not happen.')

def p_keypath_as(p):
    ''' keypath_as : keypath AS LABEL '''
    p[0] = p[1]
    p[0].label = p[3]

def p_keypath(p):
    ''' keypath : LABEL
                | keypath DOT LABEL
    '''
    if len(p) == 2:
        p[0] = treehorn.KeyPath(traversal_label=p[1])
    elif len(p) == 4:
        p[0] = p[1]
        p[0].keypath.append(p[3])
    else:
        raise Exception('This should definitely not happen.')


def p_label_list(p):
    ''' label_list : keypath_as
                   | label_list COMMA keypath_as
    '''
    if len(p) == 2:
        p[0] = {p[1].label: {'keypath': p[1].keypath, 'traversal_label': p[1].traversal_label}}
    elif len(p) == 4:
        p[0] = p[1]
        p[0][p[3].label] = {'keypath': p[3].keypath, 'traversal_label': p[3].traversal_label}
    else:
        raise Exception('What?')


class SelectHead:
    '''
    Represents the head of a SELECT clause (e.g. SELECT LABEL, LABEL FROM thing)
    '''
    def __init__(self, label_list=None, obj_name=None):
        self.label_list = label_list
        self.obj_name = obj_name

    def __repr__(self):
        out = 'Selecting: {label_list} from {obj_name}'.format(label_list=str(self.label_list), obj_name=str(self.obj_name))
        return out


def p_select_head(p):
    ''' select_head : SELECT label_list FROM LABEL
    '''
    p[0] = SelectHead(label_list=p[2], obj_name=p[4])


class SelectClause:
    def __init__(self, select_head, traversal_chain):
        self.select_head = select_head
        self.traversal_chain = traversal_chain

    def __repr__(self):
        out = 'SelectClause:\n{selections_list}\n{traversal_chain}'.format(selections_list=str(self.select_head), traversal_chain=str(self.traversal_chain))
        return out


def p_select_clause(p):
    ''' select_clause : select_head traversal_chain'''
    p[0] = SelectClause(select_head=p[1], traversal_chain=p[2])


class Query:
    def __init__(self, query_text, name=None):
        self.query_text = query_text
        self.name = name or uuid.uuid4().hex
        self.query_obj = parser.parse(query_text)
        self.relation = treehorn.Relation('foo')
        traversals = self.query_obj.traversal_chain.head.all_traversals()
        traversal_dict = {traversal.label: traversal for traversal in traversals if traversal.label is not None}
        for traversal_name, query_dict in self.query_obj.select_head.label_list.items():
            traversal_dict[query_dict['traversal_label']].update_retrieval_dict(key=traversal_name, value=query_dict['keypath'])
        self.relation.traversal = self.query_obj.traversal_chain

    def __repr__(self):
        return str(self.query_obj)


parser = yacc.yacc()

if __name__ == '__main__':
    import json
    import pprint
    obj = json.load(open('./tests/sample_data/sample_treehorn_1.json'))
    query = Query(
            'SELECT emaildict.email AS emailaddress, emaildict.username AS name, address.city AS cityname FROM obj START AT TOP GO DOWN UNTIL HAS KEY email AS emaildict '
            'GO DOWN UNTIL HAS KEY city AS address')
    for i in query.relation(obj):
        print(i)
    def non():
        relation = treehorn.Relation('foo')
        traversals = query.query_obj.traversal_chain.head.all_traversals()
        traversal_dict = {traversal.label: traversal for traversal in traversals if traversal.label is not None}
        for traversal_name, query_dict in query.query_obj.select_head.label_list.items():
            traversal_dict[query_dict['traversal_label']].update_retrieval_dict(key=traversal_name, value=query_dict['keypath'])
        relation.traversal = query.query_obj.traversal_chain
        for i in relation(obj):
            print(i)

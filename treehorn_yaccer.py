'''
The purpose of this parser is to provide a SQL-ish language for querying nested data
structures.

You could type, for example:

..

   SELECT foo, bar FROM my_obj START AT TOP GO DOWN UNTIL HAS KEY baz AS baz_label...

'''

import ply.yacc as yacc

from treehorn_parser import tokens
from metalpipe.utils import treehorn as treehorn


def p_root(p):
    '''root : condition
            | label_list
            | traversal'''
    p[0] = p[1]


def p_traversal(p):
    '''traversal : START AT condition
                 | GO DOWN UNTIL condition
                 | GO UP UNTIL condition
                 | traversal AS LABEL
                 | select_clause
    '''
    if len(p) == 4 and p[1] == 'START' and p[2] == 'AT':
        p[0] = treehorn.GoDown(condition=p[3])
    elif len(p) == 5 and p[1] == 'GO' and p[2] == 'DOWN' and p[3] == 'UNTIL':
        p[0] = treehorn.GoDown(condition=p[4])
    elif len(p) == 5 and p[1] == 'GO' and p[2] == 'UP' and p[3] == 'UNTIL':
        p[0] = treehorn.GoUp(condition=p[4])
    elif len(p) == 4 and p[2] == 'AS':
        p[0] = p[1]
        p[0] + p[3]
    elif len(p) == 2:
        p[0] = p[1]
    else:
        raise Exception('This should never happen.')


class TraversalChain:
    def __init__(self, chain=None):
        self.chain = chain

    def add_step(self, other):
        self.chain.append(other)


def p_traversal_chain(p):
    '''traversal_chain : traversal
                       | traversal_chain traversal
    '''
    if len(p) == 2:
        p[0] = TraversalChain(chain=[p[1]])
    elif len(p) == 3:
        p[0] = p[1]
        p[0].add_step(p[1])
    else:
        raise Exception('What?')



def p_condition(p):
    '''condition : LPAREN condition AND condition RPAREN
                 | LPAREN condition OR condition RPAREN
                 | NOT condition
                 | TOP
    '''
    if len(p) == 2 and p[1] == 'TOP':
        p[0] = treehorn.IsRoot()
    elif len(p) == 6 and p[3] == 'AND':
        p[0] = p[2] & p[4]
    elif len(p) == 6 and p[3] == 'OR':
        p[0] = p[2] | p[4]
    elif len(p) == 3 and p[1] == 'NOT':
        p[0] = ~ p[2]

    else:
        raise Exception('This should not happen.')


def p_label_list(p):
    ''' label_list : LABEL
                   | label_list COMMA LABEL
    '''
    if len(p) == 2:
        p[0] = [p[1]]
        print(p[0])
    else:
        p[0] = p[1] + [p[3]]
    # raise Exception('This should not happen.')


class SelectHead:
    '''
    Represents the head of a SELECT clause (e.g. SELECT LABEL, LABEL FROM thing)
    '''
    def __init__(self, label_list=None, obj_name=None):
        self.label_list = label_list
        self.obj_name = obj_name


def p_select_head(p):
    ''' select_head : SELECT label_list FROM LABEL
    '''
    p[0] = SelectHead(label_list=p[2], obj_name=p[4])


class SelectClause:
    def __init__(self, select_head, traversal_chain):
        self.select_head = select_head
        self.traversal_chain = traversal_chain


def p_select_clause(p):
    ''' select_clause : select_head traversal_chain'''
    p[0] = SelectClause(select_head=p[1], traversal_chain=p[2])


parser = yacc.yacc()

if __name__ == '__main__':
    result = parser.parse('SELECT foobar, goo FROM object START AT TOP GO DOWN UNTIL TOP')
    # result = parser.parse('NOT TOP')
    '''
    elif len(p) == 4 and p[2] == 'AS':
        p[0] = p[1]
        p[0] + p[3]  # Add the label
    '''
    pass

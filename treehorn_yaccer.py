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
    else:
        raise Exception('This should never happen.')


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


parser = yacc.yacc()

if __name__ == '__main__':
    result = parser.parse('foobar,goo')
    # result = parser.parse('NOT TOP')
    '''
    elif len(p) == 4 and p[2] == 'AS':
        p[0] = p[1]
        p[0] + p[3]  # Add the label
    '''
    pass

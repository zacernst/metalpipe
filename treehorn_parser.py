import ply.lex as lex

tokens = (
    'SELECT',
    'TOP',
    'COMMA',
    'FROM',
    'LABEL',
    'LPAREN',
    'RPAREN',
    'AND',
    'AS',
    'OR',
    'NOT',
    'START',
    'AT',
    'GO',
    'UP',
    'DOWN',
    'UNTIL',
        )

t_SELECT = r'SELECT'
t_TOP = r'TOP'
t_COMMA = r','
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_AND = r'AND'
t_OR = r'OR'
t_FROM = r'FROM'
t_NOT = r'NOT'
t_START = r'START'
t_AT = r'AT'
t_GO = r'GO'
t_UNTIL = r'UNTIL'
t_UP = r'UP'
t_DOWN = r'DOWN'
t_AS = r'AS'


def t_LABEL(t):
    r'([a-z]+)'
    return t

t_ignore = ' \t\n'
lexer = lex.lex()

if __name__ == '__main__':
    data = 'SELECT TOP'
    lexer.input(data)

    tok = lexer.token()
    while tok:
        print(tok)
        tok = lexer.token()

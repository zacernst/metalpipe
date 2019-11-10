import ply.lex as lex

tokens = (
    "SELECT",
    "TOP",
    "COMMA",
    "HAS",
    "DATA",
    "FILE",
    "LOCAL",
    "JSON",
    "KEY",
    "LABEL",
    "LPAREN",
    "SEMICOLON",
    "RPAREN",
    "DOT",
    "QUERY",
    "UNIQUE",
    "PROPERTY",
    "OF",
    "NAMED",
    "BY",
    "IS",
    "IN",
    "A",
    "ENTITY",
    "RELATED",
    "AND",
    "AS",
    "CSV",
    "COREFER",
    "OR",
    "TO",
    "NOT",
    "PYTHON",
    "FUNCTION",
    "IMPORTED",
    "FROM",
    "SOURCE",
    "START",
    "AN",
    "AT",
    "GO",
    "UP",
    "DOWN",
    "UNTIL",
)


t_QUERY = r"QUERY"
t_UNIQUE = r"UNIQUE"
t_PROPERTY = r"PROPERTY"
t_OF = r"OF"
t_NAMED = r"NAMED"
t_BY = r"BY"
t_IS = r"IS"
t_TO = r"TO"

t_DATA = r"DATA"
t_LOCAL = r"LOCAL"
t_FILE = r"FILE"
t_JSON = r"JSON"
t_CSV = r"CSV"
t_SOURCE = r"SOURCE"

t_SEMICOLON = r";"
t_COREFER = r"COREFER"
t_RELATED = r"RELATED"
t_IN = r"IN"
t_PYTHON = r"PYTHON"
t_FUNCTION = r"FUNCTION"
t_IMPORTED = r"IMPORTED"
t_FROM = r"FROM"
t_A = r"A"
t_AN = r"AN"
t_ENTITY = r"ENTITY"
t_SELECT = r"SELECT"
t_TOP = r"TOP"
t_COMMA = r","
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_AND = r"AND"
t_OR = r"OR"
t_NOT = r"NOT"
t_DOT = r"\."
t_HAS = r"HAS"
t_KEY = r"KEY"
t_START = r"START"
t_AT = r"AT"
t_GO = r"GO"
t_UNTIL = r"UNTIL"
t_UP = r"UP"
t_DOWN = r"DOWN"
t_AS = r"AS"


def t_LABEL(t):
    r"([a-z]+)"
    return t


t_ignore = " \t\n"
lexer = lex.lex()


if __name__ == "__main__":
    data = "SELECT TOP . TOP;"
    lexer.input(data)

    tok = lexer.token()
    while tok:
        print(tok)
        tok = lexer.token()

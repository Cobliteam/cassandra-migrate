# encoding: utf-8

from __future__ import print_function, unicode_literals

import re
from collections import namedtuple


class CqlSplitter(object):
    """
    Makeshift CQL parser that can only split up multiple statements.

    C* does not accept multiple DDL queries as a single string, as it can with
    DML queries using batches. Hence, we must split up CQL files to run each
    statement individually. Do that by using a simple Regex scanner, that just
    recognizes strings, comments and delimiters, which is enough to split up
    statements without tripping when semicolons are commented or escaped.
    """

    Token = namedtuple('Token', 'tpe token')

    LINE_COMMENT = 1
    BLOCK_COMMENT = 2
    STRING = 3
    SEMICOLON = 4
    OTHER = 5
    WHITESPACE = 6

    @classmethod
    def scanner(cls):
        if not getattr(cls, '_scanner', None):
            def h(tpe):
                return lambda sc, tk: cls.Token(tpe, tk)

            cls._scanner = re.Scanner([
                (r"(--|//).*?$",               h(cls.LINE_COMMENT)),
                (r"\/\*.+?\*\/",               h(cls.BLOCK_COMMENT)),
                (r'"(?:[^"\\]|\\.)*"',         h(cls.STRING)),
                (r"'(?:[^'\\]|\\.)*'",         h(cls.STRING)),
                (r"\$\$(?:[^\$\\]|\\.)*\$\$",  h(cls.STRING)),
                (r";",                         h(cls.SEMICOLON)),
                (r"\s+",                       h(cls.WHITESPACE)),
                (r".",                         h(cls.OTHER))
            ], re.MULTILINE | re.DOTALL)
        return cls._scanner

    @classmethod
    def split(cls, query):
        """Split up content, and return individual statements uncommented"""
        tokens, match = cls.scanner().scan(query)
        cur_statement = ''
        statements = []

        for i, tk in enumerate(tokens):
            if tk.tpe == cls.LINE_COMMENT:
                pass
            elif tk.tpe == cls.SEMICOLON:
                stm = cur_statement.strip()
                if stm:
                    statements.append(stm)
                cur_statement = ''
            elif tk.tpe in (cls.WHITESPACE, cls.BLOCK_COMMENT):
                cur_statement += ' '
            elif tk.tpe in (cls.STRING, cls.OTHER):
                cur_statement += tk.token

        stm = cur_statement.strip()
        if stm:
            statements.append(stm)

        return statements

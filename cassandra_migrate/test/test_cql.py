from __future__ import unicode_literals

import pytest

from cassandra_migrate.cql import CqlSplitter


@pytest.mark.parametrize('cql,statements', [
    # Two statements, with whitespace
    ('''
     CREATE TABLE hello;
     CREATE TABLE world;
     ''',
     ['CREATE TABLE hello', 'CREATE TABLE world']),
    # Two statements, no whitespace
    ('''CREATE TABLE hello;CREATE TABLE world;''',
     ['CREATE TABLE hello', 'CREATE TABLE world']),
    # Two statements, with line and block comments
    ('''
     // comment
     -- comment
     CREATE TABLE hello;
     /* comment; comment
      */
     CREATE TABLE world;
     ''',
     ['CREATE TABLE hello', 'CREATE TABLE world']),
    # Statements with semicolons inside strings
    ('''
     CREATE TABLE 'hello;';
     CREATE TABLE "world;"
     ''',
     ["CREATE TABLE 'hello;'", 'CREATE TABLE "world;"'])
])
def test_cql_split(cql, statements):
    result = CqlSplitter.split(cql.strip())
    assert result == statements

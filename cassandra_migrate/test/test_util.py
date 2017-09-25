from __future__ import unicode_literals
from builtins import str

import pytest
import yaml
from cassandra_migrate.util import YamlUnicodeLoader, cassandra_ddl_repr


TEST_YAML = """
---
hello: world
"""


def test_yaml_unicode_loader():
    data = yaml.load(TEST_YAML, Loader=YamlUnicodeLoader)

    assert data == {'hello': 'world'}
    assert all(isinstance(k, str) and isinstance(v, str)
               for k, v in data.items())


@pytest.mark.parametrize('input,output', (
    (b'hello', "'hello'"),
    ('hello', "'hello'"),
    ("'hello'", r"'''hello'''"),
    (1, '1'),
    (True, 'true'),
    (False, 'false'),
    ({}, '{}'),
    ({'hello': 1}, r"{'hello': 1}"),
    ({'hello': 1, 'world': True, 'foo': 'bar'},
     "{'foo': 'bar', 'hello': 1, 'world': true}"),
    (object, None)
))
def test_cassandra_ddl_repr(input, output):
    if output is None:
        with pytest.raises(ValueError, match=r'Cannot convert data'):
            cassandra_ddl_repr(input)
    else:
        assert cassandra_ddl_repr(input) == output

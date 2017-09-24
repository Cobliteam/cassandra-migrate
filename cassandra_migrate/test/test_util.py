from __future__ import unicode_literals
from builtins import str

import yaml
from cassandra_migrate.util import YamlUnicodeLoader


TEST_YAML = """
---
hello: world
"""


def test_yaml_unicode_loader():
    data = yaml.load(TEST_YAML, Loader=YamlUnicodeLoader)

    assert data == {'hello': 'world'}
    assert all(isinstance(k, str) and isinstance(v, str)
               for k, v in data.items())

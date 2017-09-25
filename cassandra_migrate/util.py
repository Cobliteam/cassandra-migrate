import re
from builtins import str, bytes

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader


class YamlUnicodeLoader(SafeLoader):
    def construct_yaml_str(self, node):
        return self.construct_scalar(node)


YamlUnicodeLoader.add_constructor(
    'tag:yaml.org,2002:str', YamlUnicodeLoader.construct_yaml_str)


def cassandra_ddl_repr(data):
    """Generate a string representation of a map suitable for use in C* DDL"""
    if isinstance(data, bytes):
        data = data.decode('utf-8')

    if isinstance(data, str):
        return "'" + data.replace("'", "''") + "'"
    elif isinstance(data, dict):
        pairs = []
        for k, v in sorted(data.items()):
            if not isinstance(k, str):
                raise ValueError('DDL map keys must be strings')

            pairs.append(cassandra_ddl_repr(k) + ': ' + cassandra_ddl_repr(v))
        return '{' + ', '.join(pairs) + '}'
    elif isinstance(data, bool):
        if data:
            return 'true'
        else:
            return 'false'
    elif isinstance(data, int):
        return str(data)
    else:
        raise ValueError('Cannot convert data to a DDL representation')

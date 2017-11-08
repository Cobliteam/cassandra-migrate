# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import str, open

import os
import yaml

from .migration import Migration


DEFAULT_NEW_MIGRATION_TEXT = """
/* Cassandra migration for keyspace {keyspace}.
   Version {next_version} - {date}

   {full_desc} */
""".lstrip()

DEFAULT_NEW_CQL_MIGRATION_TEXT = """
/* Cassandra migration for keyspace {keyspace}.
   Version {next_version} - {date}

   {full_desc} */
""".lstrip()

DEFAULT_NEW_PYTHON_MIGRATION_TEXT = """
# Cassandra migration for keyspace {keyspace}.
# Version {next_version} - {date}
# {full_desc}

def execute(session):
    "Main method for your migration. Do not rename this method."

    print("Cassandra session: ", session)

""".lstrip()


def _assert_type(data, key, tpe, default=None):
    """Extract and verify if a key in a dictionary has a given type"""
    value = data.get(key, default)
    if not isinstance(value, tpe):
        raise ValueError("Config error: {}: expected {}, found {}".format(
            key, tpe, type(value)))
    return value


class MigrationConfig(object):
    """
    Data class containing all configuration for migration operations

    Configuration includes:
    - Keyspace to be managed
    - Possible keyspace profiles, to configure replication in different
      environments
    - Path to load migration files from
    - Table to store migrations state in
    - The loaded migrations themselves (instances of Migration)
    """

    DEFAULT_PROFILES = {
        'dev': {
            'replication': {'class': 'SimpleStrategy', 'replication_factor': 1},
            'durable_writes': True
        }
    }

    def __init__(self, data, base_path):
        """
        Initialize a migration configuration from a data dict and base path.

        The data will usually be loaded from a YAML file, and must contain
        at least `keyspace`, `migrations_path` and `migrations_table`
        """

        self.keyspace = _assert_type(data, 'keyspace', str)

        self.profiles = self.DEFAULT_PROFILES.copy()
        profiles = _assert_type(data, 'profiles', dict, default={})
        for name, profile in profiles.items():
            self.profiles[name] = {
                'replication': _assert_type(profile, 'replication', dict),
                'durable_writes': _assert_type(profile, 'durable_writes',
                                               bool, default=True)
            }

        migrations_path = _assert_type(data, 'migrations_path', str)
        self.migrations_path = os.path.join(base_path, migrations_path)

        self.migrations = Migration.glob_all(
            self.migrations_path, '*.cql', '*.py')

        self.migrations_table = _assert_type(data, 'migrations_table', str,
                                             default='database_migrations')

        self.new_migration_name = _assert_type(
            data, 'new_migration_name', str,
            default='v{next_version}_{desc}')

        self.new_migration_text = _assert_type(
            data, 'new_migration_text', str,
            default=DEFAULT_NEW_MIGRATION_TEXT)

        self.new_cql_migration_text = _assert_type(
            data, 'new_cql_migration_text', str,
            default=DEFAULT_NEW_CQL_MIGRATION_TEXT)

        self.new_python_migration_text = _assert_type(
            data, 'new_python_migration_text', str,
            default=DEFAULT_NEW_PYTHON_MIGRATION_TEXT)

    @classmethod
    def load(cls, path):
        """Load a migration config from a file, using it's dir. as base path"""
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.load(f)

        return cls(config, os.path.dirname(path))

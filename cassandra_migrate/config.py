# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import str, open

import re
import os
from string import Formatter

import yaml
from .util import YamlUnicodeLoader, extract_config_entry
from .migration import Migration


DEFAULT_NEW_MIGRATION_TEXT = """
/* Cassandra migration for keyspace {keyspace}.
   Version {next_version} - {date}

   {full_desc} */
""".lstrip()


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
            'replication': {'class': 'SimpleStrategy', 'replication_factor': 1}
        }
    }

    MIGRATION_FORMAT_STRING_FIELDS = {'desc', 'full_desc', 'next_version',
                                      'date', 'keyspace'}

    @classmethod
    def load(cls, path):
        """Load a migration config from a file, using it's dir. as base path"""
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.load(f, Loader=YamlUnicodeLoader)

        return cls(config, os.path.dirname(path))

    def __init__(self, data, base_path):
        """
        Initialize a migration configuration from a data dict and base path.

        The data will usually be loaded from a YAML file, and must contain
        at least `keyspace`, `migrations_path` and `migrations_table`
        """

        self._formatter = Formatter()

        self.keyspace = extract_config_entry(
            data, 'keyspace', type_=str, validate=self.validate_identifier)

        user_profiles = extract_config_entry(
            data, 'profiles', type_=dict, default={})
        self.profiles = self.DEFAULT_PROFILES.copy()
        self.profiles.update((name, self.extract_profile(profile, name))
                             for name, profile in user_profiles.items())

        self.base_path = base_path

        migrations_path = extract_config_entry(
            data, 'migrations_path', type_=str)
        self.migrations_path = os.path.join(base_path, migrations_path)

        self.migrations_table = extract_config_entry(
            data, 'migrations_table', type_=str,
            validate=self.validate_identifier,
            default='database_migrations')

        self.new_migration_name = extract_config_entry(
            data, 'new_migration_name', type_=str,
            validate=self.validate_migration_format_string,
            default='v{next_version}_{desc}')

        self.new_migration_text = extract_config_entry(
            data, 'new_migration_text', type_=str,
            validate=self.validate_migration_format_string,
            default=DEFAULT_NEW_MIGRATION_TEXT)

        self.migrations = None

    def load_migrations(self):
        if self.migrations is not None:
            raise RuntimeError('Migrations have already been loaded')

        self.migrations = Migration.load_all(self.migrations_path)

    def extract_profile(self, data, name):
        prefix = 'profiles.{}.'.format(name)
        return {
            'replication': extract_config_entry(
                data, 'replication', prefix=prefix, type_=dict),
            'durable_writes': extract_config_entry(
                data, 'durable_writes', prefix=prefix, default=True,
                type_=bool)
        }

    def validate_identifier(self, value):
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', value):
            raise ValueError('Identifiers must consist of a letter followed '
                             'by letters, numbers or underscores')

    def validate_migration_format_string(self, fmt):
        for _, field_name, _, _ in self._formatter.parse(fmt):
            field_name = field_name.split('.')[0]
            if field_name not in self.MIGRATION_FORMAT_STRING_FIELDS:
                raise ValueError('Unknown format field: {}'.format(field_name))

    def format_migration_string(self, fmt, **kwargs):
        if set(kwargs.keys()) != self.MIGRATION_FORMAT_STRING_FIELDS:
            raise ValueError('Invalid keys for migration name format data')

        return self._formatter.vformat(fmt, [], kwargs)

    def next_version(self):
        if self.migrations is None:
            return 1

        return len(self.migrations) + 1

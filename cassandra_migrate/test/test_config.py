from __future__ import unicode_literals

import pytest

from cassandra_migrate.config import ConfigValidationError, MigrationConfig


@pytest.fixture
def base_config():
    return {
        'keyspace': 'test',
        'profiles': {
            'test': {
                'replication': {'class': 'SimpleStrategy',
                                'replication_factor': 1},
                'durable_writes': True
            }
        },
        'migrations_path': 'migrations',
        'migrations_table': 'test_migrations',
        'new_migration_name': 'v{next_version}_{desc}',
        'new_migration_text': 'Test Migration v{next_version}'
    }


def test_config_valid(base_config):
    cfg = MigrationConfig(base_config, '.')

    assert cfg.keyspace == 'test'
    assert cfg.profiles.keys() == ['test', 'dev']
    assert cfg.profiles['test']['replication'] == \
        base_config['profiles']['test']['replication']
    assert cfg.profiles['test']['durable_writes']
    assert cfg.migrations_path == './migrations'
    assert cfg.migrations_table == 'test_migrations'
    assert cfg.new_migration_name == 'v{next_version}_{desc}'
    assert cfg.new_migration_text == 'Test Migration v{next_version}'


@pytest.mark.parametrize('key,value', (
    ('keyspace', 1),
    ('keyspace', None),
    ('profiles', 'test'),
    ('profiles', {'test': {'replication': 'test'}}),
    ('profiles', {'test': {'replication': {}, 'durable_writes': 1}}),
    ('migrations_path', 1),
    ('migrations_path', None),
    ('migrations_table', 1),
    ('new_migration_name', 1),
    ('new_migration_name', '{bad_field}'),
    ('new_migration_text', 1),
    ('new_migration_text', '{bad_field}')
))
def test_config_invalid(base_config, key, value):
    base_config[key] = value
    with pytest.raises(ConfigValidationError) as raised:
        MigrationConfig(base_config, '.')

    exc = raised.value
    assert exc.key.startswith(key + '.') or exc.key == key

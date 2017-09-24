from __future__ import unicode_literals

import pytest

from cassandra_migrate.config import ConfigValidationError, MigrationConfig


def test_config_valid(migration_config_data):
    cfg = MigrationConfig(migration_config_data, '.')

    assert cfg.keyspace == 'test'
    assert set(cfg.profiles.keys()) == {'test', 'dev'}
    assert cfg.profiles['test']['replication'] == \
        migration_config_data['profiles']['test']['replication']
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
def test_config_invalid(migration_config_data, key, value):
    migration_config_data[key] = value
    with pytest.raises(ConfigValidationError) as raised:
        MigrationConfig(migration_config_data, '.')

    exc = raised.value
    assert exc.key.startswith(key + '.') or exc.key == key


def test_next_version_empty(migration_config_data, tmpdir):
    config = MigrationConfig(migration_config_data, str(tmpdir))
    config.load_migrations()

    assert config.next_version() == 1


def test_next_version_nonempty(migration_config_data, tmpdir):
    config = MigrationConfig(migration_config_data, str(tmpdir))

    count = 5
    migration_dir = tmpdir.join('migrations').ensure(dir=True)

    for i in range(1, 1 + count):
        f = migration_dir.join('v{:02d}.cql'.format(i))
        migration = pytest.helpers.make_migration(str(f))
        f.write_text(migration.content, 'utf-8')

    config.load_migrations()

    assert config.next_version() == count + 1

from __future__ import unicode_literals, absolute_import

import os.path

import arrow
import pytest
from cassandra_migrate.config import MigrationConfig
from cassandra_migrate.migration import Migration


@pytest.mark.parametrize('input_files,sorted_files', (
    # Numbered versions
    (('v02.py', 'v010.cql', 'v1.cql'),
     ('v1.cql', 'v02.py', 'v010.cql')),
    # Date-like versions
    (('v20170102.cql', 'v20170101b.cql', 'v20170101a.cql'),
     ('v20170101a.cql', 'v20170101b.cql', 'v20170102.cql')),
    # No version prefix and mixed number/text components
    (('1a1.cql', '2.cql', '1a2.cql', '1b02.cql', 'x.cql', '1b1.cql'),
     ('1a1.cql', '1a2.cql', '1b1.cql', '1b02.cql', '2.cql', 'x.cql'))
))
def test_sort_files(input_files, sorted_files):
    assert list(Migration.sort_files(input_files)) == list(sorted_files)


def test_load(tmpdir):
    f = tmpdir.join('test.cql')
    expected = pytest.helpers.make_migration(str(f))
    f.write_text(expected.content, 'utf-8')

    actual = Migration.load(str(f))

    assert expected == actual


def test_load_all(tmpdir):
    migrations = {}
    for name in ['v02.py', 'v1.cql', 'v10.cql']:
        path = tmpdir.join(name)
        migrations[name] = migration = pytest.helpers.make_migration(str(path))
        path.write_text(migration.content, 'utf-8')

    result = Migration.load_all(str(tmpdir), '*.cql', '*.py')
    assert result == [
        migrations['v1.cql'],
        migrations['v02.py'],
        migrations['v10.cql']
    ]


def test_generate(migration_config_data, tmpdir):
    migration_config_data['new_migration_name'] = \
        'v{next_version:02d}_{desc}_{date:YYYY-MM-DD-HH-mm-ss}_{keyspace}'
    migration_config_data['new_migration_text'] = \
        'Test: {full_desc}'
    config = MigrationConfig(migration_config_data, str(tmpdir))

    description = 'Hello, world!'
    date = arrow.get(2017, 1, 1, 0, 0, 0)

    cql_migration = Migration.generate(config, description, date=date)
    assert cql_migration.name == 'v01_Hello_world__2017-01-01-00-00-00_test.cql'
    assert cql_migration.content == 'Test: Hello, world!\n'
    assert os.path.dirname(cql_migration.path) == \
        os.path.abspath(str(tmpdir.join('migrations')))

    py_migration = Migration.generate(config, description, date=date, ext='.py')
    assert py_migration.name == 'v01_Hello_world__2017-01-01-00-00-00_test.py'
    assert cql_migration.content == 'Test: Hello, world!\n'
    assert os.path.dirname(py_migration.path) == \
        os.path.abspath(str(tmpdir.join('migrations')))


def test_persist(tmpdir):
    path = tmpdir.join('test.cql')
    migration = pytest.helpers.make_migration(str(path))
    migration.persist()

    assert path.read_text('utf-8')\
           == migration.content

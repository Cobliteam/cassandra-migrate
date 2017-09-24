from __future__ import unicode_literals, absolute_import

from collections import namedtuple

import os.path

import arrow
import pytest
from cassandra_migrate.config import MigrationConfig
from cassandra_migrate.migration import Migration
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


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


GenTestParams = \
    namedtuple('GenTestParams', 'desc,next_version,fmt,ext,date,result')


@pytest.mark.parametrize(GenTestParams._fields, (
    GenTestParams(
        desc='Hello, world!', next_version=1, ext=None, date=None,
        fmt='v{next_version:02d}_{date:YYYY-MM-DD-HH-mm-ss}_{desc}_{keyspace}',
        result='v01_2017-01-01-00-00-00_Hello_world__test.cql'),
    GenTestParams(
        desc='Hello,    world!  ', next_version=2, ext='.py', date=None,
        fmt='v{next_version:02d}_{date:YYYY-MM-DD-HH-mm-ss}_{desc}_{keyspace}',
        result='v02_2017-01-01-00-00-00_Hello_world__test.py'),
    GenTestParams(
        desc='', next_version=5, ext='.py', date=arrow.get(2017, 1, 2, 0, 0, 1),
        fmt='v{date:YYYYMMDDHHmmss}',
        result='v20170102000001.py')
))
def test_generate(migration_config_data, tmpdir, desc, next_version, fmt, ext,
                  date, result):
    migration_config_data['new_migration_name'] = fmt
    migration_config_data['new_migration_text'] = '{full_desc}'
    config = MigrationConfig(migration_config_data, str(tmpdir))

    now = arrow.get(2017, 1, 1, 0, 0, 0)
    with \
            patch('arrow.utcnow', return_value=now), \
            patch.object(config, 'next_version', return_value=next_version):

        migration = Migration.generate(config, desc, date=date, ext=ext)
        assert migration.name == result
        assert migration.content == desc + '\n'
        assert migration.path == \
            os.path.abspath(str(tmpdir.join('migrations', result)))


def test_persist(tmpdir):
    path = tmpdir.join('test.cql')
    migration = pytest.helpers.make_migration(str(path))
    migration.persist()

    assert path.read_text('utf-8') == migration.content

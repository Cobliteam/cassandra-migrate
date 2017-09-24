from __future__ import unicode_literals

import pytest
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
    f.write_binary(expected.content)

    actual = Migration.load(str(f))

    assert expected == actual


def test_load_all(tmpdir):
    migrations = {}
    for name in ['v02.py', 'v1.cql', 'v10.cql']:
        path = tmpdir.join(name)
        migrations[name] = migration = pytest.helpers.make_migration(str(path))
        path.write_binary(migration.content)

    result = Migration.load_all(str(tmpdir), '*.cql', '*.py')
    assert result == [
        migrations['v1.cql'],
        migrations['v02.py'],
        migrations['v10.cql']
    ]

from __future__ import unicode_literals
from builtins import bytes

import os.path

import pytest
from cassandra_migrate.migration import Migration, Format


@pytest.fixture
def migration_config_data():
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


@pytest.helpers.register
def make_migration(path):
    content = 'abc'
    sha256_digest = bytes.fromhex(
        'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
    _, ext = os.path.splitext(path)
    format = Format(ext)

    return Migration(name=os.path.basename(path), path=os.path.abspath(path),
                     content=content, checksum=sha256_digest, format=format)

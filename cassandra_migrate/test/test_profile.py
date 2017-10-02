from __future__ import unicode_literals
from builtins import str

import json
import os.path

import pytest
import cassandra.auth

from cassandra_migrate.config import MigrationConfig
from cassandra_migrate.error import ConfigError, ConfigValidationError
from cassandra_migrate.profile import Profile


def test_load_credentials_file_default(tmpdir):
    content = json.dumps({'user': 'user', 'password': 'pw'},
                         ensure_ascii=False)
    creds_file = tmpdir.join('creds.yml')
    creds_file.write_text(content, 'utf-8')

    cls, data = Profile.load_credentials_file(str(creds_file))
    assert cls is cassandra.auth.PlainTextAuthProvider
    assert data == {'username': 'user', 'password': 'pw'}


def test_load_credentials_file_with_provider(tmpdir):
    content = json.dumps(
        {'provider': 'SaslAuthProvider', 'arg1': 1, 'arg2': 2},
        ensure_ascii=False)
    creds_file = tmpdir.join('creds.yml')
    creds_file.write_text(content, 'utf-8')

    cls, data = Profile.load_credentials_file(str(creds_file))
    assert cls is cassandra.auth.SaslAuthProvider
    assert data == {'arg1': 1, 'arg2': 2}


def _write_creds(creds_file, data):
    content = json.dumps(data, ensure_ascii=False)
    creds_file.write_text(content, 'utf-8')
    return creds_file


def test_load_credentials_file_with_invalid_provider(tmpdir):
    f = _write_creds(tmpdir.join('creds.yml'), {'provider': 'missing'})

    with pytest.raises(ConfigError):
        Profile.load_credentials_file(str(f))


@pytest.fixture
def profile_data():
    return {
        'hosts': ['localhost'],
        'port': 9043,
        'replication': {'class': 'SimpleStrategy', 'replication_factor': 2},
        'durable_writes': False,
        'ssl_cert': './cassandra.crt',
        'cluster_options': {
            'control_connection_timeout': 5
        },
        'credentials_file': './creds.yml'
    }


@pytest.fixture
def config(migration_config_data, tmpdir):
    migration_config_data['profiles'] = {}
    return MigrationConfig(migration_config_data, str(tmpdir))


def test_from_dict(config, profile_data, tmpdir):
    creds = {'username': 'user', 'password': 'pw'}
    _write_creds(tmpdir.join('creds.yml'), creds)

    prof = Profile.from_dict(config, profile_data)
    assert prof.hosts == ('localhost',)
    assert prof.port == 9043
    assert prof.replication == {'class': 'SimpleStrategy',
                                'replication_factor': 2}
    assert (os.path.normpath(str(tmpdir.join('cassandra.crt'))) ==
            os.path.normpath(prof.ssl_cert))
    assert prof.cluster_options['control_connection_timeout'] == 5
    assert prof.auth_provider_cls is cassandra.auth.PlainTextAuthProvider
    assert prof.auth_provider_options == creds


@pytest.mark.parametrize('key,value', (
    ('hosts', 1),
    ('port', 'bad'),
    ('replication', 'bad'),
    ('durable_writes', 'bad'),
    ('ssl_cert', 1),
    ('cluster_options', 'bad'),
    ('credentials_file', 1)
))
def test_from_dict_bad_value(config, profile_data, tmpdir, key, value):
    _write_creds(tmpdir.join('creds.yml'), {'username': 'user',
                                            'password': 'pw'})

    profile_data[key] = value
    with pytest.raises(ConfigValidationError) as raised:
        Profile.from_dict(config, profile_data)

    assert raised.value.key == key
    assert raised.value.value == value

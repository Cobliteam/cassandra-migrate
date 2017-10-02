import pytest
from cassandra.auth import PlainTextAuthProvider

from cassandra_migrate.config import MigrationConfig
from cassandra_migrate.error import ConfigError
from cassandra_migrate.migrator import Migrator


@pytest.fixture
def config(migration_config_data, tmpdir):
    return MigrationConfig(migration_config_data, tmpdir)


@pytest.mark.parametrize('user,pw,result', (
    (None, None, None),
    ('user', 'pw', PlainTextAuthProvider('user', 'pw')),
    ('user', None, ConfigError),
    (None, 'pw', ConfigError)
))
def test_get_auth_provider(user, pw, result):
    if isinstance(result, type):
        with pytest.raises(result):
            Migrator.get_auth_provider(user, pw)
    else:
        provider = Migrator.get_auth_provider(user, pw)
        if result is None:
            assert provider is None
        else:
            assert isinstance(provider, type(result))
            assert provider.username == result.username
            assert provider.password == result.password


def test_init_bad_profile():
    with pytest.raises(ConfigError, matches=r'Unknown profile'):
        Migrator(config=config, profile='bad')

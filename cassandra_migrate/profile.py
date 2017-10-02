from __future__ import unicode_literals
from builtins import open, str

import os.path
from collections import namedtuple

import yaml
import cassandra.auth
from .util import YamlUnicodeLoader, extract_config_entry
from .error import ConfigError


class Profile(namedtuple('Profile',
                         ('hosts port replication durable_writes ssl_cert '
                          'cluster_options auth_provider_cls '
                          'auth_provider_options'))):
    __slots__ = ()

    @classmethod
    def load_credentials_file(cls, path):
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.load(f, Loader=YamlUnicodeLoader)

        try:
            data['username'] = data.pop('user')
        except KeyError:
            pass

        provider = data.pop('provider', 'PlainTextAuthProvider')
        try:
            provider_cls = getattr(cassandra.auth, provider)
            if not issubclass(provider_cls, cassandra.auth.AuthProvider):
                raise TypeError('Provider must be subclass of AuthProvider')
        except AttributeError:
            raise ConfigError(
                'Unknown Cassandra auth provider: {}'.format(provider))

        return provider_cls, data

    @classmethod
    def from_dict(cls, config, data, prefix=''):
        hosts = extract_config_entry(data, 'hosts', default=[], type_=list,
                                     prefix=prefix)
        hosts = tuple(hosts)

        port = extract_config_entry(data, 'port', default=9042, type_=int,
                                    prefix=prefix)

        replication = extract_config_entry(data, 'replication', default={},
                                           type_=dict, prefix=prefix)

        durable_writes = extract_config_entry(data, 'durable_writes',
                                              default=True, type_=bool,
                                              prefix=prefix)

        ssl_cert = extract_config_entry(data, 'ssl_cert', default=None,
                                        type_=str, prefix=prefix)
        if ssl_cert:
            ssl_cert = os.path.join(config.base_path, ssl_cert)

        cluster_options = extract_config_entry(data, 'cluster_options',
                                               default={}, type_=dict,
                                               prefix=prefix)

        credentials_file = extract_config_entry(data, 'credentials_file',
                                                default=None, type_=str,
                                                prefix=prefix)
        if credentials_file:
            path = os.path.join(config.base_path, credentials_file)
            auth_cls, auth_options = \
                cls.load_credentials_file(path)
        else:
            auth_cls, auth_options = None, None

        return cls(hosts=hosts, port=port, replication=replication,
                   durable_writes=durable_writes, ssl_cert=ssl_cert,
                   cluster_options=cluster_options, auth_provider_cls=auth_cls,
                   auth_provider_options=auth_options)

    def auth_provider(self):
        if not self.auth_provider_cls:
            return None

        return self.auth_provider_cls(**self.auth_provider_options)

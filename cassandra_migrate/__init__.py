# encoding: utf-8
# flake8: noqa: E402,F401

from __future__ import print_function, unicode_literals


class MigrationError(RuntimeError):
    """Base class for migration errors"""
    pass


class FailedMigration(MigrationError):
    """Database state contains failed migrations"""

    def __init__(self, version, name):
        self.version = version
        self.migration_name = name

        super(FailedMigration, self).__init__(
            'Migration failed, cannot continue '
            '(version {}): {}'.format(version, name))


class ConcurrentMigration(MigrationError):
    """Database state contains failed migrations"""

    def __init__(self, version, name):
        self.version = version
        self.migration_name = name

        super(ConcurrentMigration, self).__init__(
            'Migration already in progress '
            '(version {}): {}'.format(version, name))


class InconsistentState(MigrationError):
    """Database state differs from specified migrations"""

    def __init__(self, migration, version):
        self.migration = migration
        self.version = version

        super(InconsistentState, self).__init__(
            'Found inconsistency between specified migration and stored '
            'version: {} != {}'.format(migration, version))


class UnknownMigration(MigrationError):
    """Database contains migrations that have not been specified"""
    def __init__(self, version, name):
        self.version = version
        self.migration_name = name

        super(UnknownMigration, self).__init__(
            'Found version in database without corresponding '
            'migration (version {}): {} '.format(version, name))



from .migration import Migration
from .config import MigrationConfig
from .migrator import Migrator

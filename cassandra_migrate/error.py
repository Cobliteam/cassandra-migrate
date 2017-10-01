import os.path


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class ConfigValidationError(ConfigError):
    def __init__(self, key, value, *args):
        super(ConfigValidationError, self).__init__(*args)
        self.key = key
        self.value = value


class UnknownMigrationFormat(ConfigError):
    def __init__(self, path, *args):
        self.path = path

        _, ext = os.path.splitext(path)

        msg = "Migration has unknown extension '{}': {}".format(ext, path)
        super(ConfigError, self).__init__(msg, *args)


class MigrationError(Error):
    """Base class for migration errors"""
    pass


class FailedMigration(MigrationError):
    """Database state contains failed migrations"""

    def __init__(self, version, name, *args):
        self.version = version
        self.migration_name = name

        msg = 'Migration failed, cannot continue (version {}): {}'.format(
            version, name)
        super(FailedMigration, self).__init__(msg, *args)


class ConcurrentMigration(MigrationError):
    """Database is already in a migration process"""

    def __init__(self, version, name, *args):
        self.version = version
        self.migration_name = name

        msg = 'Migration already in progress (version {}): {}'.format(
            version, name)
        super(ConcurrentMigration, self).__init__(msg, *args)


class InconsistentState(MigrationError):
    """Database state differs from specified migrations"""

    def __init__(self, migration, version, *args):
        self.migration = migration
        self.version = version

        msg = ('Found inconsistency between specified migration and stored '
               'version: {} != {}'.format(migration, version))
        super(InconsistentState, self).__init__(msg, *args)


class UnknownMigration(MigrationError):
    """Database contains migrations that have not been specified"""
    def __init__(self, version, name, *args):
        self.version = version
        self.migration_name = name

        msg = ('Found version in database without corresponding '
               'migration (version {}): {} '.format(version, name))
        super(UnknownMigration, self).__init__(msg, *args)

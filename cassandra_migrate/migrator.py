# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import str

import re
import logging
import uuid
from collections import namedtuple
from future.moves.itertools import zip_longest

from cassandra import ConsistencyLevel, DriverException
from cassandra.cluster import Cluster

from cassandra_migrate import (Migration, MigrationConfig, FailedMigration,
                               InconsistentState)
from cassandra_migrate.cql import CqlSplitter

CREATE_MIGRATIONS_TABLE = """
CREATE TABLE {keyspace}.{table} (
    id uuid,
    version int,
    name text,
    content text,
    checksum blob,
    state text,
    applied_at timestamp,
    PRIMARY KEY (id)
) WITH caching = {{'keys': 'NONE', 'rows_per_partition': 'NONE'}};
"""

CREATE_KEYSPACE = """
CREATE KEYSPACE {keyspace}
WITH REPLICATION = {replication}
AND DURABLE_WRITES = {durable_writes};
"""

DROP_KEYSPACE = """
DROP KEYSPACE IF EXISTS "{keyspace}";
"""

CREATE_DB_VERSION = """
INSERT INTO "{keyspace}"."{table}"
(id, version, name, content, checksum, state, applied_at)
VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now())) IF NOT EXISTS
"""

FINALIZE_DB_VERSION = """
UPDATE "{keyspace}"."{table}" SET state = %s WHERE id = %s IF state = %s
"""

def cassandra_ddl_repr(data):
    """Generate a string representation of a map suitable for use in C* DDL"""
    if isinstance(data, str):
        return "'" + re.sub(r"(?<!\\)'", "\\'", data) + "'"
    elif isinstance(data, dict):
        pairs = []
        for k, v in data.items():
            if not isinstance(k, str):
                raise ValueError('DDL map keys must be strings')

            pairs.append(cassandra_ddl_repr(k) + ': ' + cassandra_ddl_repr(v))
        return '{' + ', '.join(pairs) + '}'
    elif isinstance(data, int):
        return str(data)
    elif isinstance(data, bool):
        if data:
            return 'true'
        else:
            return 'false'
    else:
        raise ValueError('Cannot convert data to a DDL representation')

class Migrator(object):
    """Execute migration operations in a C* database based on configuration.

    `opts` must contain at least the following attributes:
    - config_file: path to a YAML file containing the configuration
    - profiles: map of profile names to keyspace settings
    - user, password: authentication options. May be None to not use it.
    - hosts: comma-separated list of contact points
    - port: connection port
    """

    logger = logging.getLogger("Migrator")

    def __init__(self, config, profile='dev', hosts=['127.0.0.1'], port=9042,
                 user=None, password=None):
        self.config = config

        try:
            self.current_profile = self.config.profiles[profile]
        except KeyError:
            raise ValueError("Invalid profile name '{}'".format(profile))

        if user:
            auth_provider = PlainTextAuthProvider(user, password)
        else:
            auth_provider = None

        self.cluster = Cluster(
            contact_points=hosts,
            port=port,
            auth_provider=auth_provider,
            max_schema_agreement_wait=300,
            control_connection_timeout=10,
            connect_timeout=30)

        self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None

    def _check_cluster(self):
        """Check if the cluster is still alive, raise otherwise"""
        if not self.cluster:
            raise RuntimeError("Cluster has shut down")

    @property
    def session(self):
        """Initialize and configure a  C* driver session if needed"""

        self._check_cluster()

        if not self._session:
            s = self._session = self.cluster.connect()
            s.default_consistency_level = ConsistencyLevel.ALL
            s.default_serial_consistency_level = ConsistencyLevel.SERIAL
            s.default_timeout = 120

        return self._session

    def _get_target_version(self, v):
        """
        Parses a version specifier to an actual numeric migration version

        `v` might be:
        - None: the latest version is chosen
        - an int, or a numeric string: that exact version is chosen
        - a string: the version with that name is chosen if it exists

        If an invalid version is found, `ValueError` is raised.
        """

        if v is None:
            return len(self.config.migrations)

        if isinstance(v, int):
            num = v
        elif v.isdigit():
            num =  int(v)
        else:
            try:
                num = self.config.migrations.index(v)
            except IndexError:
                num = -1

        if num <= 0:
            raise ValueError('Invalid database version, must be a number > 0 '
                             'or the name of an existing migration')
        return num

    def _q(self, query, **kwargs):
        """
        Format a query with the configured keyspace and migration table

        `keyspace` and `table` are interpolated as named arguments
        """
        return query.format(
            keyspace=self.config.keyspace, table=self.config.migrations_table,
            **kwargs)

    def _execute(self, query, *args, **kwargs):
        """Execute a query with the current session"""

        self.logger.debug('Executing query: {}'.format(query))
        return self.session.execute(query, *args, **kwargs)

    def _ensure_keyspace(self):
        """Create the keyspace if it does not exist"""

        # Manually start session so the cluster metadata is populated
        session = self.session

        if self.config.keyspace in self.cluster.metadata.keyspaces:
            return

        self.logger.info("Creating keyspace '{}'".format(self.config.keyspace))

        query = self._q(CREATE_KEYSPACE,
            replication=cassandra_ddl_repr(self.current_profile['replication']),
            durable_writes=cassandra_ddl_repr(self.current_profile['durable_writes']))

        self._execute(query)
        self.cluster.refresh_keyspace_metadata(self.config.keyspace)

    def _ensure_table(self):
        """Create the migration table if it does not exist"""

        # Manually start session so the cluster metadata is populated
        session = self.session

        ks_metadata = self.cluster.metadata.keyspaces.get(self.config.keyspace,
                                                          None)
        # Fail if the keyspace is missing. If it should be created
        # automatically _ensure_keyspace() must be called first.
        if not ks_metadata:
            raise ValueError("Keyspace '{}' does not exist, "
                             "stopping".format(self.config.keyspace))

        table = self.config.migrations_table
        if table in ks_metadata.tables:
            return

        self.logger.info(
            "Creating table '{table}' in keyspace '{keyspace}'".format(
                keyspace=self.config.keyspace,
                table=self.config.migrations_table))

        self._execute(self._q(CREATE_MIGRATIONS_TABLE))
        self.cluster.refresh_table_metadata(self.config.keyspace, table)

    def _verify_migrations(self, migrations, ignore_failed=False):
        """Verify if the version history persisted in C* matches the migrations

        Migrations with corresponding DB versions must have the same content
        and name.
        Every DB version must have a corresponding migration.
        Migrations without corresponding DB versions are considered pending,
        and returned as a result.

        Returns a list of tuples of (version, migration), with version starting
        from 1 and incrementing linearly.
        """

        # Load all the currently existing versions and sort them by version
        # number, as Cassandra can only sort it for us by partition.
        cur_versions = self._execute(self._q(
            'SELECT * FROM "{keyspace}"."{table}"'))
        cur_versions = sorted(cur_versions, key=lambda v: v.version)

        last_version = None
        version_pairs = zip_longest(cur_versions, migrations)

        # Work through ordered pairs of (existing version, migration), so that
        # stored versions and expected migrations can be compared for any
        # differences.
        for i, (version, migration) in enumerate(version_pairs, 1):
            # If version is empty, the migration has not yet been applied.
            # Keep track of the first such version, and append it to the
            # pending migrations list.
            if not version:
                break

            # If migration is empty, we have a version in the database with
            # no corresponding file. That might mean we're running the wrong
            # migration or have an out-of-date state, so we must fail.
            if not migration:
                raise UnknownMigration(version.version, version.name)

            # A migration was previously run and failed.
            if version.state == Migration.State.FAILED:
                if ignore_failed:
                    break

                raise FailedMigration(version.version, version.name)

            last_version = version.version

            # A migration is in progress.
            if version.state == Migration.State.IN_PROGRESS:
                raise ConcurrentMigration(version.version, version.name)

            # A stored version's migrations differs from the one in the FS.
            if version.content != migration.content or \
               version.name != migration.name or \
               bytearray(version.checksum) != bytearray(migration.checksum):
                raise InconsistentState(migration, version)

        if not last_version:
            pending_migrations = list(migrations)
        else:
            pending_migrations = list(migrations)[last_version:]

        if not pending_migrations:
            self.logger.info('Database is already up-to-date')
            return []

        self.logger.info(
            'Pending migrations found. '
            'Current version: {}, Latest version: {}'.format(
            last_version, len(migrations)))

        return enumerate(pending_migrations, (last_version or 0) + 1)

    def _create_version(self, version, migration):
        """
        Write an in-progress version entry to C*

        The migration is inserted with the given `version` number if and only
        if it does not exist already (using a CompareAndSet operation).

        If the insert suceeds (with the migration marked as in-progress), we
        can continue and actually execute it. Otherwise, there was a concurrent
        write and we must fail to allow the other write to continue.

        """

        self.logger.info('Writing in-progress migration version {}: {}'.format(
            version, migration))

        version_id = uuid.uuid4()
        result = self._execute(
            self._q(CREATE_DB_VERSION),
            (version_id, version, migration.name, migration.content,
             bytearray(migration.checksum), Migration.State.IN_PROGRESS))

        if not result or not result[0].applied:
            raise ConcurrentMigration(version, migration.name)

        return version_id

    def _apply_migration(self, version, migration, skip=False):
        """
        Persist and apply a migration

        First create an in-progress version entry, apply the script, then
        finalize the entry as succeeded, failed or skipped.

        When `skip` is True, do everything but actually run the script, for
        example, when baselining instead of migrating.
        """

        self.logger.info('Advancing to version {}'.format(version))

        if skip:
            statements = []
            self.logger.info('Migration is marked for skipping, '
                         'not actually running script')
        else:
            statements = CqlSplitter.split(migration.content)

        version_uuid = self._create_version(version, migration)
        new_state = Migration.State.FAILED

        result = None
        try:
            if statements:
                self.logger.info('Executing migration - {} CQL statements'.format(
                    len(statements)))

            for statement in statements:
                self.session.execute(statement)
        except Exception as e:
            self.logger.exception('Failed to execute migration: {}', e)
            raise FailedMigration(version)
        else:
            new_state = (Migration.State.SUCCEEDED if not skip
                        else Migration.State.SKIPPED)
        finally:
            self.logger.info('Finalizing migration version with state {}'.format(
                new_state))
            result = self._execute(
                self._q(FINALIZE_DB_VERSION),
                (new_state, version_uuid, Migration.State.IN_PROGRESS))

        if not result or not result[0].applied:
            raise ConcurrentMigration(version, migration.name)

    def _advance(self, migrations, target, skip=False):
        """Apply all necessary migrations to reach a target version"""

        target_version = self._get_target_version(target)
        for version, migration in migrations:
            if version > target_version:
                break

            self._apply_migration(version, migration, skip=skip)

        self.cluster.refresh_schema_metadata()

    def baseline(self, opts):
        """Baseline a database, by advancing migration state without changes"""

        self._check_cluster()
        self._ensure_table()

        self._advance(self.config.migrations, opts.db_version, skip=True)

    def migrate(self, opts):
        """
        Migrate a database to a given version, applying any needed migrations
        """

        self._check_cluster()

        self._ensure_keyspace()
        self._ensure_table()

        migrations = self._verify_migrations(self.config.migrations,
            ignore_failed=opts.force)
        self._advance(migrations, opts.db_version)

    def reset(self, opts):
        """Reset a database, by dropping the keyspace then migrating"""
        self._check_cluster()

        self.logger.info("Dropping existing keyspace '{}'".format(
            self.config.keyspace))

        self._execute(self._q(DROP_KEYSPACE))
        self.cluster.refresh_schema_metadata()

        opts.force = False
        self.migrate(opts)

    def status(self, opts):
        """Print the current migation status of the database"""
        pass


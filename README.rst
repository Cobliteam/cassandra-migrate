Cassandra-migrate
=================

Simple Cassandra schema migration tool.

Installation
------------

Run ``pip install cassandra-migrate``, or ``python ./setup.py``

Reasoning
---------

Unlike other available tools, this one:

- Written in Python for easy installation
- Does not require ``cqlsh``, just the Python driver
- Supports baselining existing database to given versions
- Supports partial advancement
- Supports locking for concurrent instances using Lightweight Transactions
- Verifies stored migrations against configured migrations
- Stores content, checksum, date and state of every migration
- Supports deploying with different keyspace configurations for different environments

Configuration
-------------

Databases are configured through YAML files. For example:

.. code:: yaml

    keyspace: herbie
    profiles:
      prod:
        replication:
          class: SimpleStrategy
          replication_factor: 3
    migrations_path: ./migrations

Where the ``migrations`` folder (relative to the config file). contains
``.cql`` files. The files are loaded in lexical order.

The default convention is to name them in the form: ``v001_my_migration.cql``.
A custom naming scheme can be specified with the ``new_migration_name`` option.
For example

.. code:: yaml

    # Date-based migration names
    new_migration_name: "v{date:YYYYMMDDHHmmss}_{desc}"

    # Default migration names
    new_migration_name: "v{next_version:03d}_{desc}"

    # Custom initial migration content
    new_migration_text: |
      /* Cassandra migration for keyspace {keyspace}.
         Version {next_version} - {date}

         {full_desc} */

``new_migration_name`` is a new-style Python format string, which can use the
following parameters:

- ``next_version``: Number of the newly generated migration (as an ``int``).
- ``desc``: filename-clean description of the migration, as specified
  by the user.
- ``full_desc``: unmodified description, possibly containing special characters.
- ``date``: current date in UTC. Pay attention to the choice of formatting,
  otherwise you might include spaces in the file name. The above example should
  be a good starting point.
- ``keyspace``: name of the configured keyspace.

The format string should *not* contain the ``.cql`` extension, as it is added
automatically.

``new_migraton_text`` is handled with the same rules outline above, but defines
the initial content of the migration file.

Profiles
--------

Profiles can be defined in the configuration file. They can configure
the ``replication`` and ``durable_writes`` parameters for
``CREATE KEYSPACE``. A default ``dev`` profile is implicitly defined
using a replication factor of 1.

Usage
-----

Common parameters:

::

      -H HOSTS, --hosts HOSTS
                            Comma-separated list of contact points
      -p PORT, --port PORT  Connection port
      -u USER, --user USER  Connection username
      -P PASSWORD, --password PASSWORD
                            Connection password
      -c CONFIG_FILE, --config-file CONFIG_FILE
                            Path to configuration file
      -m PROFILE, --profile PROFILE
                            Name of keyspace profile to use

migrate
~~~~~~~

Advances a database to the latest (or chosen) version of migrations.
Creates the keyspace and migrations table if necessary.

Migrate will refuse to run if a previous attempt failed. To override
that after cleaning up any leftovers (as Cassandra has no DDL
transactions), use the ``--force`` option.

Examples:

.. code:: bash

    # Migrate to the latest database version using the default configuration file,
    # connecting to Cassandra in the local machine.
    cassandra-migrate -H 127.0.0.1 migrate

    # Migrate to version 2 using a specific config file.
    cassandra-migrate -c mydb.yml migrate 2

    # Migrate to a version by name.
    cassandra-migrate migrate v005_my_changes.cql

    # Force migration after a failure
    cassandra-migrate migrate 2 --force

reset
~~~~~

Reset the database by dropping an existing keyspace, then running a
migration.

Examples:

.. code:: bash

    # Reset the database to the latest version
    cassandra-migrate reset

    # Reset the database to a specifis version
    cassandra-migrate reset 3

baseline
~~~~~~~~

Advance an existing database version without actually running the
migrations.

Useful for starting to manage a pre-existing database without recreating
it from scratch.

Examples:

.. code:: bash

    # Baseline the existing database to the latest version
    cassandra-migrate baseline

    # Baseline the existing database to a specific version
    cassandra-migrate baseline 5

status
~~~~~~

Print the current status of the database.

Example:

.. code:: bash

    cassandra-migrate status

generate
~~~~~~~~

Generate a new migration file with the appropriate name and a basic header
template, in the configured ``migrations_path``.

When running the command interactively, the file will be opened by the default
editor. The newly-generated file name will be printed to stdout.

See the configuration section for details on migration naming.

Example:

.. code:: bash

    cassandra-migrate generate "My migration description"

TODO
----

-  Ask for confirmation before applying dangerous commands
-  Support Python migrations (instead of just CQL)

License (MIT)
-------------

::

    Copyright (C) 2017 Cobli

    Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

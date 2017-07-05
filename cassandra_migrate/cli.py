# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import sys
import os
import io
import logging
import argparse
import subprocess

import arrow

from . import Migrator, MigrationConfig, MigrationError

NEW_MIGRATION_TEXT = """
/* Cassandra migration for keyspace {keyspace}.
   Version {version} - {date}
   {description} */

"""

def open_file(filename):
    if sys.platform == 'win32':
        os.startfile(filename)
    else:
        if 'XDG_CURRENT_DESKTOP' in os.environ:
            opener = ['xdg-open']
        elif 'EDITOR' in os.environ:
            opener = [os.environ['EDITOR']]
        else:
            opener = ['vi']

        opener.append(filename)
        subprocess.call(opener)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("cassandra.policies").setLevel(logging.ERROR)


    parser = argparse.ArgumentParser(description='Simple Cassandra migration tool')
    parser.add_argument('-H', '--hosts', default='127.0.0.1',
                        help='Comma-separated list of contact points')
    parser.add_argument('-p', '--port', type=int, default=9042,
                        help='Connection port')
    parser.add_argument('-u', '--user',
                        help='Connection username')
    parser.add_argument('-P', '--password',
                        help='Connection password')
    parser.add_argument('-c', '--config-file', default='cassandra-migrate.yml',
                        help='Path to configuration file')
    parser.add_argument('-m', '--profile', default='dev',
                        help='Name of keyspace profile to use')

    cmds = parser.add_subparsers(help='sub-command help')

    bline = cmds.add_parser('baseline',
        help='Baseline database state, advancing migration information without '
             'making changes')
    bline.set_defaults(action='baseline')

    reset = cmds.add_parser('reset',
        help='Reset database state, by dropping the keyspace (if it exists) '
             'and recreating it from scratch')
    reset.set_defaults(action='reset')

    mgrat = cmds.add_parser('migrate',
        help='Migrate database up to the most recent (or specified) version '
             'by applying any new migration scripts in sequence')
    mgrat.add_argument('-f', '--force', action='store_true',
                        help='Force migration even if last attempt failed')
    mgrat.set_defaults(action='migrate')

    stats = cmds.add_parser('status',
        help='Print current state of keyspace')
    stats.set_defaults(action='status')

    genrt = cmds.add_parser('generate',
        help='Generate a new migration file')
    genrt.add_argument('description',
        help='Brief description of the new migration')
    genrt.set_defaults(action='generate')

    for sub in (bline, reset, mgrat):
        sub.add_argument('db_version', metavar='VERSION', nargs='?',
                         help='Database version to baseline/reset/migrate to')

    opts = parser.parse_args()
    config = MigrationConfig.load(opts.config_file)

    if opts.action == 'generate':
        clean_desc = '_'.join(opts.description.split())
        now = arrow.now()
        date = now.format()
        prefix = now.format('YYYYMMDDHHmmss')

        new_path = os.path.join(config.migrations_path,
            '{}_{}.cql'.format(prefix, clean_desc))

        with io.open(new_path, 'w', encoding='utf-8') as f:
            f.write(NEW_MIGRATION_TEXT.lstrip().format(
                keyspace=config.keyspace,
                version=next_version,
                date=date,
                description=clean_desc))

        if sys.stdin.isatty():
            open_file(new_path)

        print(os.path.basename(new_path))
    else:
        with Migrator(config=config, profile=opts.profile,
                      hosts=opts.hosts.split(','), port=opts.port,
                      user=opts.user, password=opts.password) as migrator:
            cmd_method = getattr(migrator, opts.action)
            if not callable(cmd_method):
                print('Error: invalid command', file=sys.stderr)
                sys.exit(1)

            try:
                cmd_method(opts)
            except MigrationError as e:
                print('Error: {}'.format(e), file=sys.stderr)
                sys.exit(1)



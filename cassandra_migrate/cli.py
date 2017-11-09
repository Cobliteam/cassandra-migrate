# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import sys
import os
import logging
import argparse
import subprocess

from cassandra_migrate import (Migrator, Migration, MigrationConfig,
                               MigrationError)


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

    parser = argparse.ArgumentParser(
        description='Simple Cassandra migration tool')
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
    parser.add_argument('-s', '--ssl-cert', default=None,
                        help="""
                        File path of .pem or .crt containing certificate of the
                        cassandra host you are connecting to (or the
                        certificate of the CA that signed the host certificate).
                         If this option is provided, cassandra-migrate will use
                        ssl to connect to the cluster. If this option is not
                        provided, the -k and -t options will be ignored. """)
    parser.add_argument('-k', '--ssl-client-private-key', default=None,
                        help="""
                        File path of the .key file containing the private key
                        of the host on which the cassandra-migrate command is
                        run. This option must be used in conjuction with the
                        -t option. This option is ignored unless the -s
                        option is provided.""")
    parser.add_argument('-t', '--ssl-client-cert', default=None,
                        help="""
                        File path of the .crt file containing the public
                        certificate of the host on which the cassandra-migrate
                        command is run. This certificate (or the CA that signed
                        it) must be trusted by the cassandra host that
                        migrations are run against. This option must be used in
                        conjuction with the -k option. This option is ignored
                        unless the -s option is provided.""")
    parser.add_argument('-y', '--assume-yes', action='store_true',
                        help='Automatically answer "yes" for all questions')

    cmds = parser.add_subparsers(help='sub-command help')

    bline = cmds.add_parser(
        'baseline',
        help='Baseline database state, advancing migration information without '
             'making changes')
    bline.set_defaults(action='baseline')

    reset = cmds.add_parser(
        'reset',
        help='Reset database state, by dropping the keyspace (if it exists) '
             'and recreating it from scratch')
    reset.set_defaults(action='reset')

    mgrat = cmds.add_parser(
        'migrate',
        help='Migrate database up to the most recent (or specified) version '
             'by applying any new migration scripts in sequence')
    mgrat.add_argument('-f', '--force', action='store_true',
                       help='Force migration even if last attempt failed')
    mgrat.set_defaults(action='migrate')

    stats = cmds.add_parser(
        'status',
        help='Print current state of keyspace')
    stats.set_defaults(action='status')

    genrt = cmds.add_parser(
        'generate',
        help='Generate a new migration file')
    genrt.add_argument(
        'description',
        help='Brief description of the new migration')
    genrt.add_argument(
        '--python',
        nargs='?',
        const='python',
        default='cql',
        help='Generates a python script file.')
    genrt.set_defaults(action='generate')

    for sub in (bline, reset, mgrat):
        sub.add_argument('db_version', metavar='VERSION', nargs='?',
                         help='Database version to baseline/reset/migrate to')

    opts = parser.parse_args()
    # enable user confirmation if we're running the script from a TTY
    opts.cli_mode = sys.stdin.isatty()
    config = MigrationConfig.load(opts.config_file)

    if opts.action == 'generate':
        new_path = Migration.generate(config=config,
                                      description=opts.description,
                                      output=opts.python)
        if sys.stdin.isatty():
            open_file(new_path)

        print(os.path.basename(new_path))
    else:
        with Migrator(config=config, profile=opts.profile,
                      hosts=opts.hosts.split(','), port=opts.port,
                      user=opts.user, password=opts.password,
                      host_cert_path=opts.ssl_cert,
                      client_key_path=opts.ssl_client_private_key,
                      client_cert_path=opts.ssl_client_cert) as migrator:
            cmd_method = getattr(migrator, opts.action)
            if not callable(cmd_method):
                print('Error: invalid command', file=sys.stderr)
                sys.exit(1)

            try:
                cmd_method(opts)
            except MigrationError as e:
                print('Error: {}'.format(e), file=sys.stderr)
                sys.exit(1)

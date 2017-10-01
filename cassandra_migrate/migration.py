# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import open, bytes

import re
import os
import glob
import hashlib
from collections import namedtuple
from enum import Enum

import arrow
from .error import UnknownMigrationFormat


class State(Enum):
    """Possible states of a migration, as saved in C*"""

    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    SKIPPED = 'SKIPPED'
    IN_PROGRESS = 'IN_PROGRESS'


class Format(Enum):
    """Possible migration formats by file extension"""
    CQL = '.cql'
    PYTHON = '.py'


class Migration(namedtuple('Migration',
                           'path name content checksum format')):
    """
    Data class representing the specification of a migration

    Migrations can take the form of CQL files or Python scripts, and usually
    have names starting with a version string that can be ordered.
    A checksum is kept to allow detecting changes to previously applied
    migrations"""

    __slots__ = ()

    @classmethod
    def load(cls, path, format):
        """Load a migration from a given file"""
        with open(path, 'r', encoding='utf-8') as fp:
            content = fp.read()

        return cls(path=os.path.abspath(path), name=os.path.basename(path),
                   content=content, format=format)

    @staticmethod
    def _natural_sort_key(s):
        """Generate a sort key for natural sorting"""
        k = tuple(int(text) if text.isdigit() else text
                  for text in re.split(r'([0-9]+)', s))
        return k

    @classmethod
    def sort_files(cls, paths):
        """Sort paths naturally by basename, to order by migration version"""
        return sorted(paths, key=cls._natural_sort_key)

    @classmethod
    def load_all(cls, base_path):
        """Load all paths matching the glob patterns as migrations in order"""

        files = set()
        for fmt in list(Format):
            for path in glob.iglob(os.path.join(base_path, '*' + fmt.value)):
                files.add(os.path.basename(path))

        res = []
        for f in cls.sort_files(files):
            ext = os.path.splitext(f)[-1]
            try:
                fmt = Format(ext)
            except KeyError:
                raise UnknownMigrationFormat(f)
            res.append(cls.load(os.path.join(base_path, f), format=fmt))

        return res

    @classmethod
    def generate(cls, config, description, format=None, date=None):
        """Generate a new Migration for a new version"""

        clean_desc = re.sub(r'[\W\s]+', '_', description)
        if format is None:
            format = Format.CQL
        elif not isinstance(format, Format):
            raise TypeError('Invalid format')

        ext = format.value
        if date is None:
            date = arrow.utcnow()
        next_version = config.next_version()

        format_args = {
            'desc': clean_desc,
            'full_desc': description,
            'next_version': next_version,
            'date': date,
            'keyspace': config.keyspace
        }

        fname = config.format_migration_string(
            config.new_migration_name, **format_args)
        fname += ext
        path = os.path.join(config.migrations_path, fname)

        content = config.format_migration_string(
            config.new_migration_text + '\n', **format_args)

        return Migration(name=fname, path=os.path.abspath(path),
                         content=content)

    def __new__(cls, path, name, content, checksum=None, format=Format.CQL):
        if isinstance(content, bytes):
            content_bytes = content
            content = content.decode('utf-8')
        else:
            content_bytes = content.encode('utf-8')

        if checksum is None:
            checksum = bytes(hashlib.sha256(content_bytes).digest())

        return super(Migration, cls).__new__(cls, path, name, content,
                                             checksum, format)

    def __str__(self):
        return 'Migration("{}")'.format(self.name)

    def persist(self):
        with open(os.path.abspath(self.path), 'w', encoding='utf-8') as f:
            f.write(self.content)

# encoding: utf-8

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import open, bytes

import re
import os
import glob
import hashlib
import yaml
from collections import namedtuple

class Migration(namedtuple('Migration', 'path name content checksum')):
    """
    Data class representing the specification of a migration

    Migrations can take the form of CQL files or Python scripts, and usually
    have names starting with a version string that can be ordered.
    A checksum is kept to allow detecting changes to previously applied
    migrations"""

    __slots__ = ()

    class State(object):
        """Possible states of a migration, as saved in C*"""

        SUCCEEDED = 'SUCCEEDED'
        FAILED = 'FAILED'
        SKIPPED = 'SKIPPED'
        IN_PROGRESS = 'IN_PROGRESS'

    @staticmethod
    def _natural_sort_key(s):
        """Generate a sort key for natural sorting"""
        k =  tuple(int(text) if text.isdigit() else text
                   for text in re.split(r'([0-9]+)', s))
        return k

    @classmethod
    def load(cls, path):
        """Load a migration from a given file"""
        with open(path, 'r', encoding='utf-8') as fp:
            content = fp.read()

        checksum = bytes(hashlib.sha256(content.encode('utf-8')).digest())
        return cls(os.path.abspath(path), os.path.basename(path), content,
                   checksum)

    @classmethod
    def sort_paths(cls, paths):
        """Sort paths naturally by basename, to order by migration version"""
        return sorted(paths,
                      key=lambda p: cls._natural_sort_key(os.path.basename(p)))

    @classmethod
    def glob_all(cls, base_path, *patterns):
        """Load all paths matching a glob as migrations in sorted order"""

        paths = []
        for pattern in patterns:
            paths.extend(glob.iglob(os.path.join(base_path, pattern)))

        return list(map(cls.load, cls.sort_paths(paths)))

    def __str__(self):
        return 'Migration("{}")'.format(self.name)

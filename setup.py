from setuptools import setup

VERSION = '0.3.3'

setup(name='cassandra-migrate',
      packages=['cassandra_migrate'],
      version=VERSION,
      description='Simple Cassandra database migration program.',
      long_description=open('README.rst').read(),
      url='https://github.com/Cobliteam/cassandra-migrate',
      download_url='https://github.com/Cobliteam/cassandra-migrate/archive/{}.tar.gz'.format(VERSION),
      author='Daniel Miranda',
      author_email='daniel@cobli.co',
      license='MIT',
      install_requires=[
          'cassandra-driver',
          'future',
          'PyYAML<5.0',
          'arrow',
          'tabulate'
      ],
      scripts=['bin/cassandra-migrate'],
      keywords='cassandra schema migration')

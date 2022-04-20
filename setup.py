from setuptools import setup

VERSION = '0.3.5'

install_requires = [
    "PyYAML>=5.0,<6.0",
    "arrow>=0.17,<1.0",
    "cassandra-driver>=3.0,<4.0",
    "click>=7.0,<8.0",
    "future>=0.18,<1.0",
    "geomet>=0.2,<1.0",
    "python-dateutil>=2.0,<3.0",
    "six>=1.0,<2.0",
    "tabulate>=0.8,<1.0",
    "typing-extensions>=3.0,<4.0",
]

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
      install_requires=install_requires,
      scripts=['bin/cassandra-migrate'],
      keywords='cassandra schema migration')

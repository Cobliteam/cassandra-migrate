from setuptools import setup

VERSION = '0.3.11'

install_requires = [
    "arrow==1.*",
    "cassandra-driver<4.0.0",
    "click==8.1.3",
    "future==0.18.2",
    "geomet==1.0.0",
    "python-dateutil==2.8.*",
    "PyYAML>=5,<7",
    "six==1.*",
    "tabulate==0.9.0",
    "typing-extensions>=3,<5"]

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

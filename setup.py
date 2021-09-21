from setuptools import setup

VERSION = '0.3.4'

install_requires = [
    "arrow==0.17.0",
    "cassandra-driver==3.24.0",
    "cassandra-migrate==0.3.4",
    "click==7.1.2",
    "future==0.18.2",
    "geomet==0.2.1.post1",
    "python-dateutil==2.8.1",
    "PyYAML==5.*",
    "six==1.15.0",
    "tabulate==0.8.9",
    "typing-extensions==3.7.4.3"]

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

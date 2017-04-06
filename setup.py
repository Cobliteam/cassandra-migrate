from setuptools import setup

setup(name='cassandra-migrate',
      version='0.1',
      description='Simple Cassandra database migration program',
      url='http://github.com/Cobliteam/cassandra-migrate-py',
      author='Daniel Miranda',
      author_email='daniel@cobli.co',
      license='MIT',
      packages=['cassandra_migrate'],
      install_requires=[
          'cassandra-driver',
          'future',
          'pyyaml',
          'arrow',
          'click'
      ],
      scripts=['bin/cassandra-migrate'],
      zip_safe=False)

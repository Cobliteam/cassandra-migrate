#!/bin/sh

set -e

flake8 cassandra_migrate
coverage erase
coverage run --source cassandra_migrate -m py.test
coverage report --include='cassandra_migrate/**' --omit='cassandra_migrate/test/**'

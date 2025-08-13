#!/bin/bash

# setup-slave.sh
# This script configures a PostgreSQL slave to connect to the master for replication.

# Wait for the master to be ready
echo "POSTGRES_USER is: $POSTGRES_USER"
until pg_isready -h postgres-master -p 5432 -U $POSTGRES_USER; do
  echo "Waiting for postgres-master..."
  sleep 1
done
echo "postgres-master is up and running."

# Clean old data directory
rm -rf /var/lib/postgresql/data/*

# Run base backup as 'postgres' user
su - postgres -c "PGPASSWORD=$POSTGRES_PASSWORD pg_basebackup -h postgres-master -p 5432 -U $POSTGRES_USER -D /var/lib/postgresql/data -P -Xs -R"

exec su - postgres -c "/usr/lib/postgresql/16/bin/postgres -D /var/lib/postgresql/data"
# The -R flag creates standby.signal and sets primary_conninfo automatically
# Do not start postgres manually; let the container entrypoint handle it
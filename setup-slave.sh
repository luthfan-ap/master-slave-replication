#!/bin/bash

# setup-slave.sh
# This script configures a PostgreSQL slave to connect to the master for replication.

# Wait for the master database to be available.
until pg_isready -h postgres-master -p 5432 -U user; do
  echo "Waiting for postgres-master..."
  sleep 1
done
echo "postgres-master is up and running."

# Create the recovery.conf file to enable replication
echo "standby_mode = 'on'" > /var/lib/postgresql/data/postgresql.conf
echo "primary_conninfo = 'host=postgres-master port=5432 user=replication_user password=password application_name=postgres-slave-1'" >> /var/lib/postgresql/data/postgresql.conf
echo "hot_standby = 'on'" >> /var/lib/postgresql/data/postgresql.conf
echo "recovery_target_timeline = 'latest'" >> /var/lib/postgresql/data/postgresql.conf

# Use pg_basebackup to copy the master's data directory.
# This must be run as the 'postgres' user.
su - postgres -c "pg_basebackup -h postgres-master -p 5432 -U replication_user -D /var/lib/postgresql/data -P -Xs -R"

# Start the PostgreSQL slave instance.
su - postgres -c "postgres"

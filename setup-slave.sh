#!/bin/bash
set -e

echo "Setting up PostgreSQL slave..."

# If the data directory is empty, clone from master
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "Empty data dir, cloning from master..."
  rm -rf "$PGDATA"/*
  
  # Run pg_basebackup with replication user and password
  PGPASSWORD=$POSTGRES_PASSWORD pg_basebackup \
      -h postgres-master \
      -D "$PGDATA" \
      -U $POSTGRES_USER \
      -Fp -Xs -P -R

  # Fix ownership and permissions
  chown -R postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
fi

# Start PostgreSQL with its own configs from $PGDATA
exec postgres -D "$PGDATA"

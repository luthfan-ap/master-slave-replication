#!/bin/bash
set -e

echo "Setting up PostgreSQL slave..."

# Ensure PGDATA exists
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"

if [ -z "$(ls -A "$PGDATA")" ]; then
    echo "Empty data dir, cloning from master..."

    # Run base backup from master as postgres user
    su - postgres -c "PGPASSWORD='$POSTGRES_PASSWORD' pg_basebackup \
        -h postgres-master \
        -D '$PGDATA' \
        -U '$POSTGRES_USER' \
        -v -P \
        --wal-method=stream"

    # Configure replication
    touch "$PGDATA/standby.signal"
    echo "primary_conninfo = 'host=postgres-master port=5432 user=$POSTGRES_USER password=$POSTGRES_PASSWORD'" > "$PGDATA/postgresql.auto.conf"

    # Fix permissions on data directory
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"

    echo "Slave setup complete."
else
    echo "Data directory not empty, skipping base backup."
fi

# Now hand over to postgres (not root!)
exec su - postgres -c "/usr/lib/postgresql/16/bin/postgres -D $PGDATA \
  -c config_file=/etc/postgresql/postgresql.conf \
  -c hba_file=/etc/postgresql/pg_hba.conf"
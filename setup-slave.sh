#!/bin/bash
set -e

: "${PGDATA:=/var/lib/postgresql/data}"

echo "Setting up PostgreSQL slave..."

# Ensure PGDATA exists
mkdir -p "$PGDATA"
chmod 700 "$PGDATA"

# Check if PGDATA already initialized (look for PG_VERSION file)
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    echo "Empty or invalid data dir, cloning from master..."

    until pg_isready -h postgres-master -U "$POSTGRES_USER"; do
        echo "Waiting for master to accept connections..."
        sleep 3
    done

    # Clean dir to be safe
    rm -rf "$PGDATA"/*
    chown -R postgres:postgres "$PGDATA"

    # Run base backup
    su - postgres -c "PGPASSWORD='$POSTGRES_PASSWORD' pg_basebackup \
        -h postgres-master \
        -D '$PGDATA' \
        -U '$POSTGRES_USER' \
        -v -P \
        --wal-method=stream"

    # Configure replication
    touch "$PGDATA/standby.signal"
    echo "primary_conninfo = 'host=postgres-master port=5432 user=$POSTGRES_USER password=$POSTGRES_PASSWORD'" \
      > "$PGDATA/postgresql.auto.conf"

    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"

    echo "Slave setup complete."
else
    echo "Data directory already initialized, skipping base backup."
fi

# Start postgres
exec su - postgres -c "postgres -D $PGDATA \
  -c config_file=/etc/postgresql/postgresql.conf \
  -c hba_file=/etc/postgresql/pg_hba.conf"

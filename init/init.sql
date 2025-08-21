CREATE EXTENSION IF NOT EXISTS dblink;

DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'msr-db'
   ) THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE "msr-db"');
   END IF;
END
$$;

DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_roles WHERE rolname = 'msr-user'
   ) THEN
      CREATE ROLE "msr-user" WITH LOGIN REPLICATION PASSWORD 'msr-password';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE "msr-db" TO "msr-user";

\c "msr-db"

-- coordination table for master election
CREATE TABLE IF NOT EXISTS master (
    id INT PRIMARY KEY,
    master_id TEXT,
    last_heartbeat TIMESTAMPTZ
);

-- inserting initial master record
INSERT INTO master (id, master_id, last_heartbeat)
VALUES (1, '', NOW())
ON CONFLICT (id) DO NOTHING;

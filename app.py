import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

# ========= ENV =========
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
MASTER_DB_HOST = os.getenv("MASTER_DB_HOST")
SLAVE_DB_HOST = os.getenv("SLAVE_DB_HOST")

# unique ID for the node
MY_ID = os.getenv("HOSTNAME", "unknown")

# node role
# 0 = unknown,
# 1 = master
# 2 = slave
node_role = 0

master_conn = None
slave_conn = None
conn_lock = threading.Lock()
heartbeat_thread_started = False

HEARTBEAT_INTERVAL_SEC = 2
LEADER_TTL_SEC = 10

# the name of table to store election state
ELECTION_TABLE = "master_election"


# ========= ALL ABOUT DB =========
def connect_db(host):
    return psycopg2.connect(
        host=host,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
    )


def ensure_schema(conn):
    # create initial tables
    with conn.cursor() as c:
        # Global advisory lock (choose any constant 64-bit key)
        c.execute("SELECT pg_advisory_lock(8675309)")
        try:
            # election table
            c.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ELECTION_TABLE} (
                  id INT PRIMARY KEY,
                  master_id VARCHAR(128),
                  last_heartbeat TIMESTAMPTZ
                )
                """
            )
            c.execute(
                f"""
                INSERT INTO {ELECTION_TABLE} (id, master_id, last_heartbeat)
                VALUES (1, '', NULL)
                ON CONFLICT (id) DO NOTHING
                """
            )
            # app demo table
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS test_table (
                  id BIGSERIAL PRIMARY KEY,
                  content TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        finally:
            c.execute("SELECT pg_advisory_unlock(8675309)")
    conn.commit()


# ========= HEARTBEAT =========
def send_heartbeat():
    global node_role, master_conn
    while True:
        if node_role == 1 and master_conn and not master_conn.closed:
            try:
                with conn_lock:
                    with master_conn.cursor() as c:
                        c.execute(
                            f"UPDATE {ELECTION_TABLE} "
                            "SET last_heartbeat = NOW() "
                            "WHERE id = 1 AND master_id = %s",
                            (MY_ID,),
                        )
                    master_conn.commit()
            except Exception as e:
                print(f"[{MY_ID}] Heartbeat failed: {e}")
                node_role = 2  # demote; election loop will handle recovery
                try:
                    master_conn.close()
                except Exception:
                    pass
                master_conn = None
        time.sleep(HEARTBEAT_INTERVAL_SEC)


# ========= ELECTION =========
def election_loop():
    global node_role, master_conn, heartbeat_thread_started

    while True:
        try:
            # (re)connect to master DB if needed
            if master_conn is None or master_conn.closed:
                master_conn = connect_db(MASTER_DB_HOST)
                ensure_schema(master_conn)

            # Read current leader record
            with conn_lock:
                with master_conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        f"SELECT master_id, last_heartbeat FROM {ELECTION_TABLE} WHERE id = 1"
                    )
                    row = c.fetchone() or {"master_id": "", "last_heartbeat": None}
            current_master = row["master_id"]
            last_heartbeat = row["last_heartbeat"]

            # compare datetime
            now = datetime.now(timezone.utc)
            leader_dead = (
                last_heartbeat is None
                or (now - last_heartbeat) > timedelta(seconds=LEADER_TTL_SEC)
            )

            if current_master == "" or leader_dead:
                # try to claim leadership atomically with TTL condition
                with conn_lock:
                    with master_conn.cursor() as c:
                        c.execute(
                            f"""
                            UPDATE {ELECTION_TABLE}
                            SET master_id = %s, last_heartbeat = NOW()
                            WHERE id = 1
                              AND (master_id = '' OR last_heartbeat IS NULL
                                   OR last_heartbeat < NOW() - INTERVAL '{LEADER_TTL_SEC} seconds')
                            """,
                            (MY_ID,),
                        )
                    master_conn.commit()

                # read again to confirm who is leader after the race
                with conn_lock:
                    with master_conn.cursor(cursor_factory=RealDictCursor) as c:
                        c.execute(
                            f"SELECT master_id FROM {ELECTION_TABLE} WHERE id = 1"
                        )
                        row2 = c.fetchone() or {"master_id": ""}
                if row2["master_id"] == MY_ID:
                    if node_role != 1:
                        print(f"[{MY_ID}] Became the leader")
                    node_role = 1
                    if not heartbeat_thread_started:
                        threading.Thread(target=send_heartbeat, daemon=True).start()
                        heartbeat_thread_started = True
                else:
                    node_role = 2
            else:
                node_role = 1 if current_master == MY_ID else 2

        except Exception as e:
            print(f"[{MY_ID}] Election error: {e}")
            node_role = 2
            if master_conn:
                try:
                    master_conn.close()
                except Exception:
                    pass
            master_conn = None

        time.sleep(5)


# ========= READ PATH (SLAVE) =========
def get_slave_conn():
    global slave_conn
    if slave_conn is None or slave_conn.closed:
        slave_conn = connect_db(SLAVE_DB_HOST)
    return slave_conn


# ========= CLI =========
def command_loop():
    global master_conn, slave_conn, node_role
    while True:
        try:
            command = input("Enter command (read/write/exit): ").strip().lower()
        except EOFError:
            print("Exiting due to non-interactive input.")
            break

        if command == "exit":
            print("Exiting...")
            sys.exit(0)

        elif command == "write":
            if node_role != 1:
                print("This node is not the leader. Writes are not allowed.")
                continue
            try:
                with conn_lock:
                    with master_conn.cursor() as c:
                        c.execute(
                            "INSERT INTO test_table (content, created_at) VALUES (%s, NOW())",
                            (f"Hello from {MY_ID}",),
                        )
                    master_conn.commit()
                print(f"[{MY_ID}] Write successful")
            except Exception as e:
                print(f"[{MY_ID}] Write failed: {e}")
                try:
                    master_conn.close()
                except Exception:
                    pass
                master_conn = None

        elif command == "read":
            try:
                conn = get_slave_conn()
                with conn.cursor() as c:
                    c.execute(
                        "SELECT id, content, created_at "
                        "FROM test_table ORDER BY created_at DESC LIMIT 5"
                    )
                    for row in c.fetchall():
                        print(row)
            except Exception as e:
                print(f"[{MY_ID}] Read failed: {e}")
                try:
                    slave_conn.close()
                except Exception:
                    pass
                slave_conn = None

        else:
            print(f"Unknown command {command}. Please try again.")


# ========= MAIN =========
if __name__ == "__main__":
    threading.Thread(target=election_loop, daemon=True).start()
    command_loop()
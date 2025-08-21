import os
import sys
import threading
import time
import psycopg2 #for the postgresql connection
from datetime import datetime, timezone

# setting an empty variable for the role (1: master, 2: slave)
node_role = "unassigned"

# now for the connection, each for the master and slave
master_conn = None
slave_conn = None

conn_lock = threading.Lock() # lock for the connection

db_name = os.getenv("PG_DB", "msr-db")
db_user = os.getenv("PG_USER", "msr-user")
db_password = os.getenv("PG_PASSWORD", "msr-password")
master_host = os.getenv("MASTER_DB_HOST", "postgres-master")
slave_host = os.getenv("SLAVE_DB_HOST", "postgres-slave-1")

HEARTBEAT_TTL = int(os.getenv("HEARTBEAT_TTL", "5"))  # in seconds

# node name for election
NODE_NAME = os.getenv("APP_NAME", os.getenv("HOSTNAME", "unknown-node"))

# to connect with the db host
def get_connection(db_host):
    return psycopg2.connect(
        host=db_host, database=db_name, user=db_user, password=db_password
    )
    
def wait_for_connection(host, retries=10, delay=3):
    for i in range(retries):
        try:
            conn = get_connection(host)
            return conn
        except Exception as e:
            print(f"[{NODE_NAME}] Waiting for {host}... ({i+1}/{retries}) {e}")
            time.sleep(delay)
    raise Exception(f"Could not connect to {host} after {retries} attempts")

# creates 2 tables:
# - table for storage (key, value)
# - table for master (id, master_id) 
def init_db():
    global master_conn, slave_conn

    try:
        master_conn = wait_for_connection(master_host)
        with conn_lock, master_conn.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS storage (key TEXT PRIMARY KEY, value TEXT)"
            )
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS master (
                        id SERIAL PRIMARY KEY,
                        master_id TEXT,
                        last_heartbeat TIMESTAMPTZ
                    )"""
            )
            cursor.execute(
                "INSERT INTO master (id, master_id, last_heartbeat) VALUES (1, '', now()) "
                "ON CONFLICT (id) DO NOTHING"
            )
        master_conn.commit()
        print(f"[{NODE_NAME}] Connected to master DB.")
    except Exception as e:
        print(f"[{NODE_NAME}] Could not connect to master DB after retries: {e}")

    try:
        slave_conn = wait_for_connection(slave_host)
        print(f"[{NODE_NAME}] Connected to slave DB.")
    except Exception as e:
        print(f"[{NODE_NAME}] Could not connect to slave DB: {e}")

# elects master
def master_election():
    global master_conn, node_role

    try:
        with conn_lock, master_conn.cursor() as cursor:
            # check the current master
            cursor.execute("SELECT master_id, last_heartbeat FROM master WHERE id=1;")
            row = cursor.fetchone()
            now = datetime.now(timezone.utc)

            if row:
                current_master, last_heartbeat = row
                expired = (
                    last_heartbeat is None
                    or (now - last_heartbeat).total_seconds() > HEARTBEAT_TTL
                )
                
                # if theres no master, claim the master role.
                if expired or current_master == "":
                    cursor.execute(
                        "UPDATE master SET master_id = %s, last_heartbeat = %s "
                        "WHERE id = 1 AND (master_id = '' OR master_id = %s)",
                        (NODE_NAME, now, NODE_NAME),
                    )
                    master_conn.commit()
                    node_role = "master"
                    print(f"[{NODE_NAME}] became the MASTER.")
                
                elif current_master == NODE_NAME:
                    node_role = "master"
                else:
                    node_role = "slave"
            
            # if theres another master, set the role to slave
            else:
                node_role = "slave"

    except Exception as e:
        print(f"[{NODE_NAME}] Election error: {e}")
        if master_conn:
            master_conn.rollback()

# loops in the background, checks whether there is an active master or not
def async_master_loop():
    global master_conn, node_role

    while True:
        if master_conn is None:
            time.sleep(2)
            continue
        
        master_election()

        if node_role == "master":
            try:
                with conn_lock, master_conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE master SET last_heartbeat = %s "
                        "WHERE id = 1 AND master_id = %s",
                        (datetime.now(timezone.utc), NODE_NAME),
                    )
                master_conn.commit()
                print(f"[{NODE_NAME}] sent heartbeat.")
            except Exception as e:
                print(f"[{NODE_NAME}] Heartbeat error: {e}")
                master_conn.rollback()
        time.sleep(2)

# 'put' command handler
def put(key, value):
    global master_conn
    if master_conn is None:
        print(f"[{NODE_NAME}] No master connection.")
        return
    with conn_lock, master_conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO storage (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (key, value),
        )
    master_conn.commit()
    print(f"Successfully stored (key: {key}, value: {value})")

# 'get' command handler
def get(key):
    global master_conn, slave_conn, node_role

    conn = master_conn if node_role == "master" else slave_conn
    
    if conn is None:
        print("No active connection to the database.")
        return None
    
    with conn_lock, conn.cursor() as cursor:
        cursor.execute("SELECT value FROM storage WHERE key = %s", (key,))
        row = cursor.fetchone()
    val = row[0] if row else None
    print(f"[{NODE_NAME}] GET {key} -> {val}")
    return val
            

# main function
def main():
    global node_role

    init_db()
    t = threading.Thread(target=async_master_loop, daemon=True)
    t.start()

    if sys.stdin.isatty():
        while True:
            try:
                print(f"Current node role : {node_role}")
                command = input("Enter command : ").strip()
                if command == ".exit":
                    print(f"[{NODE_NAME}] Exiting.")
                    break
                elif command == "put":
                    k = input("Key: ")
                    v = input("Value: ")
                    put(k, v)
                elif command == "get":
                    k = input("Key: ")
                    get(k)
                else:
                    print(f"[{NODE_NAME}] Unknown command {command}. Please try again.")
            except KeyboardInterrupt:
                print(f"[{NODE_NAME}] Exiting...")
                break
    else:
        # non-interactive: so the service will stay alive
        while True:
            time.sleep(5)

if __name__ == "__main__":
    main()
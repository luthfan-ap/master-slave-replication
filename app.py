import os
import sys
import threading
import time
import psycopg2 #for the postgresql connection

# setting an empty variable for the role (1: master, 2: slave)
node_role = 0

# now for the connection, each for the master and slave
master_conn = None
slave_conn = None

conn_lock = threading.Lock() # lock for the connection


# to connect with the db host
def db_connect(db_host):
    try:
        db_name = os.getenv('DB_NAME', 'storage_db')
        db_user = os.getenv('DB_USER', 'user')
        db_password = os.getenv('DB_PASSWORD', 'password')

        return psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host
        )
    except psycopg2.OperationalError as e:
        print(f"Cannot connect to the database at {db_host}: {e}", file=sys.stderr)
        return None
    

# creates 2 tables:
# - table for storage (key, value)
# - table for master (id, master_id) 
def create_table(conn):
    try:
        with conn.cursor() as cursor:
            # Storage table (key, value)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS storage (
                    key VARCHAR(50) PRIMARY KEY,
                    value TEXT
                );
            """)

            # Master table (id, master_id)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS master (
                    id INT PRIMARY KEY,
                    master_id VARCHAR(128)
                );
            """)

            # inserting a single row in master(id, master_id) table
            cursor.execute("""
                INSERT INTO master (id, master_id) VALUES (
                    1,
                    ''
                ) ON CONFLICT (id) DO NOTHING;
            """)

        conn.commit() # committing the database changes made.
        print("Database tables are set.")
    
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
        conn.rollback()


# elects master
def master_election():
    global node_role
    my_id = os.getenv('HOSTNAME') or os.getenv('CONTAINER_ID') or 'unknown_id'

    try:
        with conn_lock:
            with master_conn:
                with master_conn.cursor() as cursor:
                    # check the current master
                    cursor.execute("SELECT master_id FROM master WHERE id=1;")
                    row = cursor.fetchone()
                    current = row[0] if row else ''

                    # if theres no master, claim the master role.
                    cursor.execute("""
                        UPDATE master
                        SET master_id = %s
                        WHERE id = 1 AND (master_id = '' OR master_id = %s)
                        RETURNING master_id;
                    """, (my_id, my_id))
                    updated = cursor.fetchone()

        # update role based on the election result
        if updated and updated[0] == my_id:
            node_role = 1  # aku master
            return True
        else:
            # if theres another master, set the role to slave
            node_role = 2
            return False
    except psycopg2.Error as e:
        print(f"error during election: {e}")
        node_role = 2
        return False


# loops in the background, checks whether there is an active master or not
def async_master_loop():
    global master_conn, node_role
    my_id = os.getenv('HOSTNAME') or os.getenv('CONTAINER_ID') or 'unknown_id'
    while True:
        if master_conn is None:
            master_conn = db_connect(os.getenv('MASTER_DB_HOST'))
        else:
            try:
                with conn_lock:
                    with master_conn.cursor() as cursor:
                        cursor.execute("SELECT master_id FROM master WHERE id=1;")
                        row = cursor.fetchone()
                        current = row[0] if row else ''
                if current == '':
                    master_election()  # do the master election
                elif current == my_id:
                    node_role = 1
                else:
                    node_role = 2
            except psycopg2.Error as e:
                print(f"Election loop DB error: {e}")
                node_role = 2
        time.sleep(5)

# 'put' command handler
def put(key, value):
    global master_conn

    try:
        with master_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO storage (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;",
                (key, value)
            )
            master_conn.commit()
            print(f"Successfully stored (key: {key}, value: {value})")

    except psycopg2.Error as e:
        print(f"Error storing data: {e}")
        master_conn.rollback()

# 'get' command handler
def get(key):
    global master_conn
    global slave_conn
    global node_role

    conn = master_conn if node_role == 1 else slave_conn
    
    if conn is None:
        print("No active connection to the database.")
        return None
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT value FROM storage WHERE key = %s;", (key,))
            result = cursor.fetchone()

            if result:
                print(f"Retrieved value for key '{key}': {result[0]} (from {node_role} node)")
                return result[0]
            else:
                print("Key not found.")
                return None
            
    except psycopg2.Error as e:
        print(f"Error retrieving data: {e}")
        return None


# main function
def main():
    global master_conn
    global slave_conn
    global node_role

    # making the connections to the master and slave databases
    master_conn = db_connect(os.getenv('MASTER_DB_HOST'))
    slave_conn = db_connect(os.getenv('SLAVE_DB_HOST'))

    # exit if theres no connection
    if not master_conn or not slave_conn:
        sys.exit(1)
    
    # create the initial tables
    create_table(master_conn)

    # start the master election loop in a separate thread
    election_thread = threading.Thread(target=async_master_loop, daemon=True)
    election_thread.start()
    print("Master election loop started.")

    while True:
        try:
            print(f"Current node role: {node_role}") # Display the current node role
            # Display commands based on the node role
            if node_role == 1:
                print(
                    """
                    Commands:
                    1. put <key> <value>
                    2. get <key>
                    3. .exit / .quit
                    """
                )
            elif node_role == 2:
                print(
                    """
                    Commands:
                    1. get <key>
                    2. .exit / .quit
                    """
                )
            elif node_role == 0:
                print("Node role is not set. Waiting for master election...")
                time.sleep(1)
                continue
            
            print("Enter command: ")
            statement = sys.stdin.readline().strip()
            if not statement:
                print("Exiting due to non-interactive input.")
                time.sleep(1)
                continue

            parts = statement.split()
            command = parts[0].lower()

            # put command
            if command == "put":
                if node_role == 1:
                    if len(parts) >= 3:
                        key = parts[1]
                        value = " ".join(parts[2:])
                        put(key, value)
                    else:
                        print("Usage: put <key> <value>")
                else:
                    print("Only the master node can execute 'put' command.")
            
            # get command
            elif command == "get":
                if len(parts) != 2:
                    print("Usage: get <key>")
                    continue
                key = parts[1]
                get(key)

            # exit commands
            elif command in (".exit", ".quit"):
                print("Exiting...")
                break
            
            else:
                print("Unknown command {command}. Please try again.")

        except (IOError, EOFError):
            break
        except Exception as e:
            print(f"An error occurred: {e}")

    if master_conn:
        master_conn.close()
    if slave_conn:
        slave_conn.close()

if __name__ == "__main__":
    main()
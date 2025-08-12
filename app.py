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
                    master_id VARCHAR (50)
                );
            """)

            # inserting a single row in master(id, master_id) table
            cursor.execute("""
                INSERT INTO master (id, master_id) VALUES (
                    1,
                    ''
                );
            """)

        conn.commit() # committing the database changes made.
        print("Database tables are set.")
    
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
        conn.rollback()


# elects master
def master_election():
    global master_conn
    global node_role

    try:
        with master_conn.cursor() as cursor:
            # locking the row (lock with the FOR UPDATE statement)
            cursor.execute(
                "SELECT master_id FROM master WHERE id = 1 FOR UPDATE;"
            )

            # updating the master_id to the elected master id
            master_id = os.getenv('HOSTNAME') or os.getenv('CONTAINER_ID') or 'unknown_id'
            cursor.execute(
                "UPDATE master SET master_id = %s WHERE id = 1;", (master_id,)
            )
            master_conn.commit()

            # set the node_role into 1 (master role)
            if node_role != 1:
                node_role = 1
            return True

    except psycopg2.OperationalError as e:
        print("Lost connection to the master DB during election: {e}")
        return False
    except psycopg2.Error as e:
        print("error during election: {e}")
        # set the node_role into 2 (follower role)
        node_role = 2
        return False


# loops in the background, checks whether there is an active master or not
def async_master_loop():
    global master_conn
    global node_role

    while True:
        if node_role != 1: # if not master yet
            if master_conn:
                master_election()
            else:
                master_conn = db_connect(os.getenv('MASTER_DB_HOST'))
        # re-loop every 5 seconds
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
def get(key)

# main function
def main()
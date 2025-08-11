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
def db_connection(db_host):
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
# - table for leader (id, leader_id) 
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

            # Leader table (id, leader_id)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leader (
                    id INT PRIMARY KEY,
                    leader_id VARCHAR (50)
                );
            """)

            # inserting a single row in leader(id, leader_id) table
            cursor.execute("""
                INSERT INTO leader (id, leader_id) VALUES (
                    1,
                    ''
                );
            """)

        conn.commit() # committing the database changes made.
        print("Database tables are set.")
    
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
        conn.rollback()

# elects leader
def leader_election():
    

# loops in the background, checks whether there is an active leader or not
def async_leader_loop()

# 'put' command handler
def put(key, value)

# 'get' command handler
def get(key)

# main function
def main()
import os
import sys
import threading
import time
import psycopg2 #for the postgresql connection

# setting an empty variable for the role (master / slave)
node_role = ""

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
def create_table(conn)

# elects leader
def leader_election()

# loops in the background, checks whether there is an active leader or not
def async_leader_loop()

# 'put' command handler
def put(key, value)

# 'get' command handler
def get(key)

# main function
def main()
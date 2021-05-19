import psycopg2
from psycopg2 import connect, Error

# defaults
DB_HOST = "127.0.0.1"
DB_NAME = "sparkifydb"
DB_USER = "student"
DB_PASSWORD = "student"
DB_PORT = '5432'

def get_conn(host=None, db=None, user=None, password=None, port=None):
    """
    Connect to database and return a connection and cursor tuple
    """
    
    if not db:
        db = DB_NAME
    if not host:
        host = DB_HOST
    if not user:
        user = DB_USER
    if not password:
        password = DB_PASSWORD
    if not port:
        port = DB_PORT
    #print(f'Coonection success: host={host} dbname={db} user={user} password={password} port={port}')
    conn = connect(f"host={host} dbname={db} user={user} password={password} port={port}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return (conn, cur)
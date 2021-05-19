# import the connect library from psycopg2
from psycopg2 import connect, Error
from sql_queries import create_table_queries, drop_table_queries
#Import Database connections
from db_conn import get_conn


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    try:       
        #Get connection to default DB
        conn, cur = get_conn(db = "studentdb", host='192.168.0.126')     
        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        # close connection to default database
        cur.close()
        conn.close() 
        print('DB sparkify - created successfully')   
    except Error as error:
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print('Tables DROP success')
    except Error as error:
        print('Error in dropping Tables')
        print(error)

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    
    try:
        conn, cur = get_conn(host='192.168.0.126')
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print('Tables CREATE success')
    except Error as error:
        print('Error in dropping Tables')
        print(error)

def main():
    """
    - Drops (if exists) and Creates the sparkify database.     
    - Establishes connection with the sparkify database and gets cursor to it.      
    - Drops all the tables.     
    - Creates all tables needed.     
    - Finally, closes the connection. 
    """
    create_database()

    #Get connection to default DB
    conn, cur = get_conn(host='192.168.0.126')

    drop_tables(cur, conn)
    create_tables(cur, conn)
       
    # close connection to database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
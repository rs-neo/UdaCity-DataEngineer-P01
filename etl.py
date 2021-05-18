import os
import glob
from psycopg2 import connect, Error, OperationalError, errorcodes
import pandas as pd
from sql_queries import *
from io import StringIO
from db_conn import get_conn


def show_exception(error):
    """
    Handles and parses psycopg2 exceptions
    """
    print ("\nExtensions.Diagnostics:", error.diag)    
    # print the pgcode and pgerror exceptions
    print ("PGerror:", error.pgerror)
    print ("PGcode:", error.pgcode, "\n")

def del_duplicates(cur, conn):
    """
    Delete duplicate records in each table using the queries in `del_duplicate_records` list.
    """
    try:
        for query in del_duplicate_records:
            print('Duplicates Deleted')
            cur.execute(query)
            conn.commit()
    except OperationalError as error:
        print('Error in deleting Duplicate Records')
        # pass exception to function
        show_exception(error)

def copy_from_stringio(cur, df, table, col):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    f = StringIO()
    df.to_csv(f, index=False, header=False)
    f.seek(0)
    
    try:
        cur.copy_from(f, table, sep=",", columns=col)
    except OperationalError as error:        
        # pass exception to function
        show_exception(error)
        # set the connection to 'None' in case of error
        cur.close()
        return 1

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[:,['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[:,['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    #Create new time dataframe
    time_data = {"timestamp": t, "hour": t.dt.hour, "day": t.dt.day, "week of year": t.dt.strftime('%U'), "month": t.dt.month, "year": t.dt.year, "weekday": t.dt.weekday}
    time_df = pd.concat(time_data, axis=1)
    # insert time data records
    col = ('start_time' ,'hour','day','week','month','year','weekday')
    copy_from_stringio(cur, time_df, 'time', col)

    # load user table
    user_df = df.loc[:,['userId','firstName','lastName','gender','level']].drop_duplicates(subset='userId',keep='first')
    col = ('user_id','first_name','last_name', 'gender','level')
    # insert user records
    copy_from_stringio(cur, user_df, 'users',col)
    
    # insert songplay records
    for index, row in df.iterrows():    
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'],row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed. - {datafile}')


def main():
    try:
        #Connection to database and get cursor
        conn, cur = get_conn(host = "192.168.0.126")
        #Process logfiles and data files
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
        #Clear duplicates in user table after memory dump.
        del_duplicates(cur, conn)
    except OperationalError as error:
        # pass exception to function
        show_exception(error)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
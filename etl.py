import os
import glob
from psycopg2 import connect, Error, OperationalError, errorcodes
import pandas as pd
from sql_queries import *
from db_conn import get_conn


def show_exception(error):
    """
    Handles and parses psycopg2 exceptions
    """
    print ("\nExtensions.Diagnostics:", error.diag)    
    # print the pgcode and pgerror exceptions
    print ("PGerror:", error.pgerror)
    print ("PGcode:", error.pgcode, "\n")

def process_song_file(cur, filepath):
    """
    Parses and processes a JSON formatted metaData file
    Uses Pandas module to read log file and create DataFrame
    Artists and Songs tables are updated using data from log file
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as error:
        print(f"Error reading Songs data files: {error}")
        exit()

    # insert song record
    try:
        song_data = df.loc[:,['song_id','title','artist_id','year','duration']].values.tolist()[0]
        cur.execute(song_table_insert, song_data)
        print('Song data INSERT success')
    except OperationalError as error:
        # pass exception to function
        show_exception(error) 
    
    # insert artist record
    try:
        artist_data = df.loc[:,['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
        cur.execute(artist_table_insert, artist_data)
        print('Artist data INSERT success')
    except OperationalError as error:
        # pass exception to function
        show_exception(error)

def process_log_file(cur, filepath):
    """
    Parses and processes a JSON formatted log file
    Uses Pandas module to read log file and create DataFrame
    Function derives various date and time attributes using Pandas dt method on timestamp field in log file
    User and Time tables are updated using data from log file
    Songplays facts table is updated using data from log file along with song_id and artist_id queried from song and artist tables, respectively
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']
    #t = pd.Timestamp(df['ts'])
    #t = pd.Timestamp(t)
    
    #Create new time dataframe
    time_data = {"timestamp": t, "hour": t.dt.hour, "day": t.dt.day, "week of year": t.dt.strftime('%U'), "month": t.dt.month, "year": t.dt.year, "weekday": t.dt.weekday}
    time_df = pd.concat(time_data, axis=1)
    
    # insert time data records
    col = ('start_time' ,'hour','day','week','month','year','weekday')
    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
        print("Time table INSERT statement successful.")
    except Error as error:
        print(f"Time table INSERT ERROR: {error}")
        exit()

    # load user table
    user_df = df.loc[:,['userId','firstName','lastName','gender','level']].drop_duplicates(subset='userId',keep='first')
    col = ('user_id','first_name','last_name', 'gender','level')
    # insert user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
        print("User table INSERT statement successful.")
    except Error as error:
        print(f"User table INSERT ERROR: {error}")
        exit()
    
    # insert songplay records
    try:
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
    except Error as error:
        print(f"Songplays table INSERT ERROR: {error}")
        exit()

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
        print(f'{i}/{num_files} files processed.')


def main():
    try:
        #Connection to database and get cursor
        conn, cur = get_conn()
        #Process logfiles and data files
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    except OperationalError as error:
        # pass exception to function
        show_exception(error)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
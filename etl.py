import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    process song files and pushes the data to the song and artist tables. 

    read data into a dataframe in a given file and pushes the relevant data to song and artist dimension tables. 

    Parameters
    ----------

    cur: database cusrsor 
    filepath: path to a songs datafile
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)
    for index, entry in df.iterrows():
        # insert song record
        song_data = df.filter(items=["song_id","title","artist_id", "year","duration"]).values[index].tolist()
        cur.execute(song_table_insert, song_data)
    
        # insert artist record
        artist_data = df.loc[index, \
                             ["artist_id","artist_name","artist_location","latitude","longitude"]] \
                                    .values.tolist()
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    process log files and pushes the data to the time, users  and songplays tables. 

    read data into a dataframe in a given file and pushes the relevant data to time and users dimension tables, 
    as the data for the artist_id and song_id is not available in the log_file and needed for songplays table, we fetch them 
    from the songs and artists tables.  

    Parameters
    ----------

    cur: database cusrsor 
    filepath: path to a log datafile
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong" ]

    # convert timestamp column to datetime
    df['ts'] =pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df.ts, df.ts.dt.hour,  df.ts.dt.day,  df.ts.dt.week,  df.ts.dt.month,  df.ts.dt.year,  df.ts.dt.weekday)
    column_labels = ("start_time", "hour","day","week","month","year","weekday")
    time_df = pd.DataFrame(columns=column_labels)
    for index, column_label in enumerate(column_labels):
        time_df[column_label]=time_data[index]
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]
    user_df.dropna(inplace = True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, \
                     songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    '''
    get all json files from a given directory calls the relevant processing function

    This function gets the path to all the .json files in a given directory and depending on the 
    func parameter calls the relevant processing function passing the paths to the files to it.

    Parameters
    ----------

    cur: cursor
    conn: connection to the database
    filepath: filepath to the directory where the data files reside
    func: could be process_song_file or process_log_file 
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    main function of the etl.py

    This function creates the connection to the db, gets the cursor and calls 
    the process data two times. once to process log files and another to process log files
    then it closes the connection to the database

    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
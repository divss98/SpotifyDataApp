# -*- coding: utf-8 -*-
"""
DSCI 551 Project: Database Creation
02/17/2021
"""

#package to connect to PostgreSQL
import psycopg2

#function to load data to table after table creation
def load_to_table(data_file, table_name, sep = ','):
    with open(data_file, 'r', encoding='latin_1') as f:
        next(f) # Skip the header row.
        cur.copy_from(f, table_name, sep=sep)

#connect to database
con = psycopg2.connect(database="postgres", user="postgres", password="dsci551", host="localhost", port="5432")
cur = con.cursor()

#create audio table
cur.execute("""
    DROP TABLE IF EXISTS audio;
    CREATE TABLE audio(
    track_id text PRIMARY KEY,
    track_duration integer,
    danceability float,
    energy float,
    loudness float,
    speechiness float,
    acousticness float,
    instrumentalness float,
    liveness float,
    valence float,
    tempo float)
""")
#load csv files created from Project_1_DataCollection.py to table
load_to_table('data/audio.csv', 'audio', sep=',')

#create track table
cur.execute("""
    DROP TABLE IF EXISTS track;
    CREATE TABLE track(
    track_id text PRIMARY KEY,
    track_name text,
    track_artist_id text,
    track_album_id text,
    track_popularity integer,
    track_explicit text)
""")
load_to_table('data/track.csv', 'track', sep='\t')

#create album table
cur.execute("""
    DROP TABLE IF EXISTS album;
    CREATE TABLE album(
    album_id text PRIMARY KEY,
    album_name text,
    album_release_date text,
    artist_id text)
""")
load_to_table('data/album.csv', 'album', sep='\t')

#create artist table
cur.execute("""
    DROP TABLE IF EXISTS artist;
    CREATE TABLE artist(
    artist_id text PRIMARY KEY,
    artist_name text,
    artist_genre text,
    artist_popularity integer,
    artist_followers integer)
""")
load_to_table('data/artist.csv', 'artist', sep='\t')
con.commit() #to complete the transaction

print("Data has been loaded to PostgreSQL tables.")

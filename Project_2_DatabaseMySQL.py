# -*- coding: utf-8 -*-
"""
DSCI 551 Project: Database Creation
02/17/2021
"""
import os
os.chdir(r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project")

#import packages to connect to MySQL
import mysql.connector #pip install mysql-connector-python as well
import pandas as pd
from sqlalchemy import create_engine
import pymysql

#read in datasets from Data Collection
album = pd.read_csv('final_data/album.csv', sep =',')
artist = pd.read_csv('final_data/artist.csv', sep =',')
track = pd.read_csv('final_data/track.csv', sep =',')
audio = pd.read_csv('final_data/audio.csv', sep =',')
final = pd.read_csv('final_data/final.csv', sep =',')

#connect to main database (mysql) to create a new database (mydatabase)
con = mysql.connector.connect(
    host='localhost',
    #database='mysql',
    user='root',
    password='dsci551')

cur = con.cursor()
try: #if mydatabase exists already, then skip
    cur.execute("CREATE DATABASE mydatabase;")
except:
    print("mydatabase is already created. Skipping step.")
con.commit() #to complete the transaction

#connect to new database (mydatabase)
con = mysql.connector.connect(
    host='localhost',
    database='mydatabase',
    user='root',
    password='dsci551')

#create sqlalchemy engine to import data to tables
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"  
                      .format(user="root", pw="dsci551", 
                      db="mydatabase"))
#insert dataframes to MySQL tables
album.to_sql('album', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
artist.to_sql('artist', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
track.to_sql('track', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
audio.to_sql('audio', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
final.to_sql('final', con = engine, if_exists = 'replace', chunksize = 1000,index=False)

#add constraints to tables
cur = con.cursor()
cur.execute("ALTER TABLE artist MODIFY artist_id varchar(25);")
cur.execute("ALTER TABLE album MODIFY album_id varchar(25);")
cur.execute("ALTER TABLE album MODIFY album_artist_id varchar(25);")
cur.execute("ALTER TABLE track MODIFY track_id varchar(25);")
cur.execute("ALTER TABLE track MODIFY track_artist_id varchar(25);")
cur.execute("ALTER TABLE track MODIFY track_album_id varchar(25);")
cur.execute("ALTER TABLE audio MODIFY track_id varchar(25);")

#add primary keys... can't add foreign keys because not all values in child table would be in parent
cur.execute("ALTER TABLE artist ADD PRIMARY KEY (artist_id);")
cur.execute("ALTER TABLE album ADD PRIMARY KEY (album_id);")
cur.execute("ALTER TABLE track ADD PRIMARY KEY (track_id);")
cur.execute("ALTER TABLE audio ADD PRIMARY KEY (track_id);")


#cur.execute("ALTER TABLE roster ADD FULLTEXT index_name(Name);")
#cur.execute(f'SELECT * FROM roster WHERE MATCH (name) AGAINST ("{search_name}" IN NATURAL LANGUAGE MODE);')


print("Data has been loaded to MySQL tables.")

#show that the process worked
print("These are the tables in the database:")
cur.execute("SHOW TABLES;")
for x in cur:
    print(x)
    
print("Testing: print two rows from album table")    
cur.execute("SELECT * FROM album limit 2;")
myresult = cur.fetchall()
for x in myresult:
  print(x)    

con.commit()


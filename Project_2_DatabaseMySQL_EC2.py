# -*- coding: utf-8 -*-
"""
DSCI 551 Project: Database Creation
02/17/2021
"""
import os
#os.chdir(r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project")

#import packages to connect to MySQL
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import pymysql

#read in datasets from Data Collection
album = pd.read_csv('data/album.csv', sep ='\t')
artist = pd.read_csv('data/artist.csv', sep ='\t')
track = pd.read_csv('data/track.csv', sep ='\t')
audio = pd.read_csv('data/audio.csv', sep =',')

#connect to main database (mysql) to create a new database (mydatabase)
con = mysql.connector.connect(
    host='localhost',
    database='mysql',
    user='root',
    password='protossx1')

cur = con.cursor()
try: #if mydatabase exists already, then skip
    cur.execute("CREATE DATABASE mydatabase")
except:
    print("mydatabase is already created. Skipping step.")
con.commit() #to complete the transaction

#connect to new database (mydatabase)
con = mysql.connector.connect(
    host='localhost',
    database='mydatabase',
    user='root',
    password='protossx1')

#create sqlalchemy engine to import data to tables
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"  
                      .format(user="root", pw="protossx1", 
                      db="mydatabase", auth_plugin='mysql_native_password'))
#insert dataframes to MySQL tables
album.to_sql('album', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
artist.to_sql('artist', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
track.to_sql('track', con = engine, if_exists = 'replace', chunksize = 1000,index=False)
audio.to_sql('audio', con = engine, if_exists = 'replace', chunksize = 1000,index=False)

print("Data has been loaded to MySQL tables.")


#show that the process worked
cur = con.cursor()

print("These are the tables in the database:")
cur.execute("SHOW TABLES")
for x in cur:
    print(x)
    
print("Testing: print two rows from album table")    
cur.execute("SELECT * FROM album limit 2")
myresult = cur.fetchall()
for x in myresult:
  print(x)    

con.commit()


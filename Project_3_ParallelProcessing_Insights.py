# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 15:17:44 2021

@author: griz1
"""

#### idea: save all of these results as csvs or something
#### then have another script to load in csv results and create tables/charts so that she can run this script...

import os
os.chdir(r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project\spark_df")

#import packages
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plt.style.use('ggplot')

mydict = {}
directory = r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project\spark_df"
for folder in os.listdir(directory):
    directory2 = directory + '\\' + folder
    for filename in os.listdir(directory2):
        if filename.endswith('.csv'):
            #print(filename)
            mydict[folder] = pd.read_csv(directory2 + '\\' + filename) #there is only 1 file per folder
#mydf = pd.read_csv(r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project\spark_df\audio_stats\part-00000-11c767c3-b365-47fa-98d9-fb7a10d1491b-c000.csv")

#load in pyspark csv files
audio_stats = mydict['audio_stats']
most_tracks_by_artist = mydict['most_tracks_by_artist']
oldest_newest_albums = mydict['oldest_newest_albums']
top_followers_by_artist = mydict['top_followers_by_artist']
top_popularity_by_artist = mydict['top_popularity_by_artist']


## show statistics/visualizations

#1. table of top 10 artists by popularity and by followers and scatterplot of artist popularity vs artist followers
top_popularity_by_artist.head(10)
top_followers_by_artist.head(10)

plt.figure(figsize=(600/96, 400/96), dpi=96)
for i in range(len(top_popularity_by_artist)):
    #initiate plot
    plt.plot(top_popularity_by_artist.loc[i,"artist_popularity"], top_popularity_by_artist.loc[i,"artist_followers"], "o", ms=7)
    #show labels for top 10 and bottom 5
    if i in list(range(0,10)) or i in list(range(len(top_popularity_by_artist)-5,len(top_popularity_by_artist))):
        plt.text(x=top_popularity_by_artist.loc[i,"artist_popularity"], y=top_popularity_by_artist.loc[i,"artist_followers"], s=top_popularity_by_artist.loc[i,"artist_name"], fontsize=7)
    #plt.xlim(0,100)
    plt.xlabel("Artist Popularity")
    plt.ylabel("Artist Followers (per 100K)")
    plt.title("Scatterplot of Artist Popularity vs. Artist Followers")


#2. table and bar chart of top 10 artists with most tracks
most_tracks_by_artist.head(10)

plt.figure(figsize=(500/96, 300/96), dpi=96)
plt.bar(most_tracks_by_artist['artist_name'][0:10], most_tracks_by_artist['cnt'][0:10], color=[np.random.rand(3,) for i in range(len(most_tracks_by_artist['artist_name'][0:10]))])
plt.xticks(rotation=90)
plt.xlabel("Artist Name")
plt.ylabel("Count of Tracks")
plt.title("Bar Chart of Number of Tracks by Artist")
plt.show()


#3. table of oldest and latest albums released -- first 10 rows are oldest, last 10 are latest
oldest_newest_albums


#4. table and radar chart of audio feature statistics for top 9 artists by average popularity
audio_stats.head(9)

#create a function to make the radar charts
def radar_chart(data, nrow, ncol, index, title):
    df = pd.DataFrame({"feat": ["energy","danceability","valence","acousticness","speechiness"], "vals": [data.loc[index,"avg_energy"],data.loc[index,"avg_danceability"],data.loc[index,"avg_valence"],data.loc[index,"avg_acousticness"],data.loc[index,"avg_speechiness"]]})
    
    #initiate plot
    ax = plt.subplot(nrow, ncol, index+1, polar=True)
    theta = np.arange(len(df) + 1) / float(len(df)) * 2 * np.pi
    vals = df["vals"].values
    vals = np.append(vals, vals[0])
    ax.plot(theta, vals, color=np.random.rand(3,), marker="o")
    ax.fill(theta, vals, "green", alpha=0.2)
    
    #set labels and title
    plt.xticks(theta[:-1], df["feat"], color='grey', size=9)
    plt.yticks([0,0.25,0.5,0.75,1], ["0","0.25","0.5","0.75","1"], color="black", size=7)
    plt.ylim(0,1)
    plt.title(f"{title}", size=10)

#plot all charts
plt.figure(figsize=(900/96, 900/96), dpi=96)
for i in range(0,9):
    radar_chart(data = audio_stats[0:9], nrow=3, ncol=3, index=i, title=audio_stats.loc[i,"artist_name"])



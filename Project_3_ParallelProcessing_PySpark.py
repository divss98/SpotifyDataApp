# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 10:18:36 2021

@author: griz1
"""

import os
os.chdir(r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project")

#import packages
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, DateType
#from pyspark.conf import SparkConf
import pyspark.sql.functions as fc
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("Dsci551Project").getOrCreate()
print(spark)

# rdd=spark.sparkContext.parallelize([1,2,3,4,5])
# rdd.sum()


##do a test of below first by reading in data as csv files then... use MySQL connector maybe..
#load track data
track_schema = StructType() \
      .add("track_id",StringType(),True) \
      .add("track_name",StringType(),True) \
      .add("track_artist_id",StringType(),True) \
      .add("track_album_id",StringType(),True) \
      .add("track_number",IntegerType(),True) \
      .add("track_popularity",IntegerType(),True) \
      .add("track_explicit",StringType(),True) \
      .add("track_homepage",StringType(),True)
track_spark = spark.read.format("csv") \
      .option("header", True) \
      .schema(track_schema) \
      .load("final_data/track.csv")
track_spark.printSchema()

#load album data
album_schema = StructType() \
      .add("album_id",StringType(),True) \
      .add("album_name",StringType(),True) \
      .add("album_release_date",DateType(),True) \
      .add("album_total_tracks",IntegerType(),True) \
      .add("album_artist_id",StringType(),True) \
      .add("album_image",StringType(),True) \
      .add("album_homepage",StringType(),True)
album_spark = spark.read.format("csv") \
      .option("header", True) \
      .schema(album_schema) \
      .load("final_data/album.csv")
album_spark.printSchema()

#load audio data
audio_spark = spark.read.csv("final_data/audio.csv", header=True, inferSchema=True)
audio_spark.printSchema()

#load artist data
artist_spark = spark.read.csv("final_data/artist.csv", header=True, inferSchema=True)
artist_spark.printSchema()


###summary statistics we want to show -- this will be a static page?
# top 10 artists by number of tracks:
# select art.artist_name, count(t.track_id)
# from track t
# left join artist art on art.artist_id = t.track_artist_id
# where art.artist_name is not null
# group by art.artist_name
# order by count(t.track_id) desc
# limit 10
most_tracks = track_spark.join(artist_spark, artist_spark.artist_id == track_spark.track_artist_id, how='left').filter("artist_name is not null") \
    .groupBy('artist_name').agg(fc.count("*").alias("cnt")).orderBy(fc.desc('cnt'))#.limit(10)
most_tracks_by_artist = most_tracks #.toPandas()
most_tracks_by_artist.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv("spark_df/most_tracks_by_artist")


# top 10 artists by popularity / followers... popularity based on music being streamed -- make scatterplot?
# ^^make scatterplot of popularity and followers?
# #select artist_name, artist_popularity, artist_followers
# from artist
# order by artist_popularity desc
# limit 10
# select artist_name, artist_popularity, artist_followers
# from artist
# order by artist_followers desc
# limit 10
most_artists_fol = artist_spark.select('artist_name','artist_popularity','artist_followers').orderBy(fc.desc('artist_followers'))#.limit(10)
top_followers_by_artist = most_artists_fol #.toPandas()
top_followers_by_artist.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv("spark_df/top_followers_by_artist")
most_artists_pop = artist_spark.select('artist_name','artist_popularity','artist_followers').orderBy(fc.desc('artist_popularity'))#.limit(10)
top_popularity_by_artist = most_artists_pop #.toPandas()
top_popularity_by_artist.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv("spark_df/top_popularity_by_artist")

# top 10 oldest and newest albums... can create bar chart of release decade?
# (select alb.album_name, art.artist_name, alb.album_release_date, art.artist_genre, alb.album_image, alb.album_homepage
# from album alb
# left join artist art on art.artist_id = alb.album_artist_id
# order by alb.album_release_date asc
# limit 10)
# union all
# (select alb.album_name, art.artist_name, alb.album_release_date, art.artist_genre, alb.album_image, alb.album_homepage
# from album alb
# left join artist art on art.artist_id = alb.album_artist_id
# where art.artist_name is not null
# order by alb.album_release_date desc
# limit 10)
oldest_album_date = album_spark.join(artist_spark, artist_spark.artist_id == album_spark.album_artist_id, how='left').filter('artist_name is not null')
oldest_album_date2 = oldest_album_date.select('album_name', 'artist_name', 'album_release_date').orderBy(fc.asc('album_release_date')).limit(10)
#oldest_album_date = album_release2.toPandas()
newest_album_date = album_spark.join(artist_spark, artist_spark.artist_id == album_spark.album_artist_id, how='left').filter('artist_name is not null')
newest_album_date2 = newest_album_date.select('album_name', 'artist_name', 'album_release_date').orderBy(fc.desc('album_release_date')).limit(10)
#newest_album_date = album_release4.toPandas()
oldest_newest_union = oldest_album_date2.union(newest_album_date2)
oldest_newest_albums = oldest_newest_union #.toPandas()
oldest_newest_albums.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv("spark_df/oldest_newest_albums")

#top 10 artists by average popularity and their average audio feature scores
# select art.artist_name, avg(t.track_popularity), avg(t.track_explicit), avg(aud.track_duration)/1000 as 'track_duration_sec', 
# 	avg(aud.danceability) as 'danceability', avg(aud.energy) as 'energy', avg(aud.loudness) as 'loudness', 
#     avg(aud.speechiness) as 'speechiness', avg(aud.acousticness) as 'acousticness', avg(aud.instrumentalness) as 'instrumentalness', 
#     avg(aud.liveness) as 'liveness', avg(aud.valence) as 'valence', avg(aud.tempo) as 'tempo', art.artist_homepage
# from track t
# left join audio aud on aud.track_id = t.track_id
# left join album alb on alb.album_id = t.track_album_id
# left join artist art on art.artist_id = t.track_artist_id
# group by art.artist_name
# order by avg(t.track_popularity) desc
# limit 10
audio_stats = track_spark.join(audio_spark, audio_spark.track_id == track_spark.track_id, how='left') \
    .join(album_spark, album_spark.album_id == track_spark.track_album_id, how='left') \
    .join(artist_spark, artist_spark.artist_id == track_spark.track_artist_id, how='left') \
    .groupBy('artist_name').agg(fc.mean('track_popularity').alias("avg_popularity"),(fc.mean('track_duration')/100).alias("avg_duration_sec") \
    ,fc.mean('danceability').alias("avg_danceability") \
    ,fc.mean('energy').alias("avg_energy") \
    ,fc.mean('loudness').alias("avg_loudness") \
    ,fc.mean('speechiness').alias("avg_speechiness") \
    ,fc.mean('acousticness').alias("avg_acousticness") \
    ,fc.mean('liveness').alias("avg_liveness") \
    ,fc.mean('valence').alias("avg_valence") \
    ,fc.mean('tempo').alias("avg_tempo")) \
    .orderBy(fc.desc('avg_popularity'))#.limit(15) #.toPandas()
audio_stats.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv("spark_df/audio_stats")
 



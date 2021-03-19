# -*- coding: utf-8 -*-
"""
DSCI 551 Project: Data Collection from API
02/17/2021
"""

#import packages
import pandas as pd
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.client import SpotifyException
import os
import string

print(os.getcwd())
os.chdir(r"C:\Users\griz1\Documents\Grad School\USC\Spring 2021\DSCI 551\Project")

#initialize Spotify API
cid = 'a5465019d284413898d6128ad598b776'
secret = 'c53fe03eabf64c8ea31d53c31613df28'
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

#plan
#-> get random artists
#-> get a few albums from each artist 
#-> get all songs from each album
#-> get audio features from each song

# get random artists
artist_id = []
artist_name = []
artist_genre = []
artist_popularity = []
artist_followers = []
artist_image = []
artist_homepage = []

#NOTE: the artist search is random so the results can be different 
letters_list = list(string.ascii_lowercase)
for letter in letters_list:
    search_artist = sp.search(q=f'artist:{letter}', type="artist", limit=20, market="US")
    for artist in range(len(search_artist['artists']['items'])):
        spot_id = search_artist['artists']['items'][artist]['id']
        name = search_artist['artists']['items'][artist]['name']
        
        if name not in artist_name and "artist" not in name.lower():
            try:
                genre = search_artist['artists']['items'][artist]['genres']#[0]
            except IndexError: #some artists might not have a genre
                genre = [] #''
            try:
                image = search_artist['artists']['items'][artist]['images'][0]['url']
            except IndexError:
                image = ''
            popularity = search_artist['artists']['items'][artist]['popularity']
            followers = search_artist['artists']['items'][artist]['followers']['total']
            homepage = search_artist['artists']['items'][artist]['external_urls']['spotify']
            
            artist_id.append(spot_id)
            #artist_data['artist_id'].append(spot_id) <-- for some reason includes duplicates, so use list instead
            artist_name.append(name)
            artist_genre.append(genre)
            artist_popularity.append(popularity)
            artist_followers.append(followers)
            artist_image.append(image)
            artist_homepage.append(homepage)

#convert lists to dataframe
artist_df = pd.DataFrame(
    {'artist_id': artist_id,
     'artist_name': artist_name,
     'artist_genre': artist_genre,
     'artist_popularity': artist_popularity,
     'artist_followers': artist_followers,
     'artist_image': artist_image,
     'artist_homepage': artist_homepage
    })
print("Artist data collected.")

#-> get a few albums from each artist 
album_id = []
album_name = []
album_release_date = []
album_total_tracks = []
album_artist_id = []
album_image = []
album_homepage = []

for i in range(len(artist_id)):
    search_album = sp.artist_albums(artist_id[i], album_type='album', limit=10)
    for j in range(len(search_album['items'])):
        spot_id = search_album['items'][j]['id']
        name = search_album['items'][j]['name']
        
        if name not in album_name:
            try:
                image = search_album['items'][j]['images'][0]['url']
            except IndexError:
                image = ''
            art_id = search_album['items'][j]['artists'][0]['id']
            release_date = search_album['items'][j]['release_date']
            total_tracks = search_album['items'][j]['total_tracks']
            homepage = search_album['items'][j]['external_urls']['spotify']
            
            album_id.append(spot_id)
            album_name.append(name)
            album_release_date.append(release_date)
            album_total_tracks.append(total_tracks)
            album_artist_id.append(art_id)
            album_image.append(image)
            album_homepage.append(homepage)

#convert lists to dataframe
album_df = pd.DataFrame(
    {'album_id': album_id,
     'album_name': album_name,
     'album_release_date': album_release_date,
     'album_total_tracks': album_total_tracks,
     'album_artist_id': album_artist_id,
     'album_image': album_image,
     'album_homepage': album_homepage
    })
print("Album data collected.")

track_id = []
track_name = []
track_artist_id = []
track_album_id = []
track_number = []
track_popularity = []
track_explicit = []
track_homepage = [] #no image for tracks

track_features = { #audio features
    'track_id':[],
    'track_duration':[],
    'danceability':[], 'energy':[],
    'loudness':[], 'speechiness':[],'acousticness':[], 'instrumentalness':[], 
    'liveness':[], 'valence':[], 'tempo':[],
    'key':[], 'mode':[], 'time_signature':[], 
    }

#-> get all songs from each album
#-> get audio features from each song
for i in range(len(album_id)):
    search_tracks = sp.album_tracks(album_id[i])
    print(i)
    if i in [*range(25,750,30)]:
        time.sleep(3)
    for j in range(len(search_tracks['items'])):
        spot_id = search_tracks['items'][j]['id']
        art_id = search_tracks['items'][j]['artists'][0]['id']
        name = search_tracks['items'][j]['name']
        if name not in track_name:
            track_id.append(spot_id)
            track_name.append(name)
            track_artist_id.append(art_id)
            track_album_id.append(album_id[i])
            
            #get additional track info
            search_track = sp.track(spot_id)
            number = search_track['track_number']
            popularity = search_track['popularity']
            explicit = search_track['explicit']
            homepage = search_track['external_urls']['spotify']
            track_number.append(number)
            track_popularity.append(popularity)
            track_explicit.append(explicit)
            track_homepage.append(homepage)
            
            #get audio features for each track
            search_features = sp.audio_features(spot_id)
            try:
                track_features['track_id'].append(search_features[0]['id'])
            except TypeError as e:
                print(e)
                continue
            except SpotifyException as e:
                print(e)
                continue
            else:
                track_features['track_duration'].append(search_features[0]['duration_ms'])
                track_features['danceability'].append(search_features[0]['danceability'])
                track_features['energy'].append(search_features[0]['energy'])
                track_features['loudness'].append(search_features[0]['loudness'])
                track_features['speechiness'].append(search_features[0]['speechiness'])
                track_features['acousticness'].append(search_features[0]['acousticness'])
                track_features['instrumentalness'].append(search_features[0]['instrumentalness'])
                track_features['liveness'].append(search_features[0]['liveness'])
                track_features['valence'].append(search_features[0]['valence'])
                track_features['tempo'].append(search_features[0]['tempo'])
                track_features['key'].append(search_features[0]['key'])
                track_features['mode'].append(search_features[0]['mode'])
                track_features['time_signature'].append(search_features[0]['time_signature'])  

#convert lists and dictionary to dataframe
track_df = pd.DataFrame(
    {'track_id': track_id,
     'track_name': track_name,
     'track_artist_id': track_artist_id,
     'track_album_id': track_album_id,
     'track_number': track_number,
     'track_popularity': track_popularity,
     'track_explicit': track_explicit,
     'track_homepage': track_homepage
    })
audio_df = pd.DataFrame(track_features)
print("Track data collected.")
print("Audio data collected.")

#practice joining them together
final_df = pd.merge(track_df, audio_df, how='left', left_on='track_id', right_on='track_id',
      suffixes=('_x', '_y')) #only 1 song without audio features
final_df2 = pd.merge(final_df, album_df, how='left', left_on='track_album_id', right_on='album_id',
      suffixes=('_x', '_y'))
final_df3 = pd.merge(final_df2, artist_df, how='left', left_on='track_artist_id', right_on='artist_id',
      suffixes=('_x', '_y'))

#save dataframes as csv file... note separator is tab or comma
os.makedirs('data4', exist_ok=True)
final_df3.to_csv(r"data4/final.csv", index=False,encoding="utf-8-sig", sep =',')
artist_df.to_csv(r"data4/artist.csv", index=False,encoding="utf-8-sig", sep =',')
album_df.to_csv(r"data4/album.csv", index=False,encoding="utf-8-sig", sep =',')
track_df.to_csv(r"data4/track.csv", index=False,encoding="utf-8-sig", sep =',')
audio_df.to_csv(r"data4/audio.csv", index=False,encoding="utf-8-sig", sep =',')

print("Please find the collected data in the data folder.")

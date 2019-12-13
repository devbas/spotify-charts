import pandas as pd
import os
from glob import glob
import multiprocessing as mp
import sys 
import getopt 
import requests
import multiprocessing as mp
import urllib
import re 
import json
mp.set_start_method('spawn', True)

spotify_search_url = 'https://api.spotify.com/v1/search'
input_directory = ''

def auth_get(url, spotify_acces_token): 
  print('url: ', url)
  result = requests.get(url, headers={"Authorization": "Bearer {}".format(spotify_acces_token)}).json()
  return result

    
def retrieve_audio_features(track_ids, spotify_acces_token):
  try:
    request_url = 'https://api.spotify.com/v1/audio-features/?ids={0}'.format(','.join(track_ids))
  
    features = auth_get(request_url, spotify_acces_token)
    return features
  except Exception as e: 
    print('Exception: ', e)


def collect_audio_features(features):
  global audio_features_df 
  
  if not features['audio_features']: 
    print('features: ', features)
  
  audio_features_df = pd.DataFrame(columns=[
    'acousticness', 
    'danceability', 
    'duration_ms', 
    'energy', 
    'instrumentalness',
    'spotify_id', 
    'pitch_key', 
    'liveness', 
    'loudness', 
    'mode', 
    'speechiness', 
    'tempo', 
    'time_signature', 
    'valence'
  ])

  for feature in features['audio_features']: 
    try: 
      audio_features_df = audio_features_df.append({
        'acousticness': feature['acousticness'],
        'danceability': feature['danceability'],
        'duration_ms': feature['duration_ms'],
        'energy': feature['energy'],
        'instrumentalness': feature['instrumentalness'],
        'spotify_id': feature['id'],
        'pitch_key': feature['key'],
        'liveness': feature['liveness'],
        'loudness': feature['loudness'],
        'mode': feature['mode'],
        'speechiness': feature['speechiness'],
        'tempo': feature['tempo'],
        'time_signature': feature['time_signature'],
        'valence': feature['valence']
      }, ignore_index=True)
    except TypeError:
      print('typeerror feature: ', feature)
  
  audio_features_df.to_csv('audio_features.csv', mode='a', header=False)

if __name__ == '__main__':

  spotify_acces_token = ''
  extension = '*.csv'

  try: 
    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv,"d:t:",["directory","token"])
  except getopt.GetoptError as err:
    print('err: ', err)
    print('retrieve-sentiment.py -d <csv directory> -t <spotify access token>')
    sys.exit(2)
  
  for opt, arg in opts: 
    if opt in ('-d', '--directory'): 
      input_directory = arg
    elif opt in ('-t', '--token'): 
      spotify_acces_token = arg
    else: 
      print('retrieve-sentiment.py -d <csv directory> -t <spotify access token>')
      sys.exit()

  
  try: 
    
    unique_tracks_df = pd.DataFrame(columns=['Track Name', 'Artist', 'URL'])

    csv_files = [file for path, subdir, files in os.walk(input_directory) 
                for file in glob(os.path.join(input_directory, extension))]
  
    for file in csv_files:
      csv_df = pd.read_csv(file, header=0, usecols=['Track Name', 'Artist', 'URL'])
      unique_csv_df = csv_df.drop_duplicates(subset=['Track Name', 'Artist'])
      unique_tracks_df = pd.concat([unique_tracks_df, unique_csv_df]).drop_duplicates().reset_index(drop=True)

    unique_tracks_df['spotify_id'] = [re.sub('^https://open.spotify.com/track/', '', str(track['URL']))
      for index, track in unique_tracks_df.iterrows()]

    pool = mp.Pool(mp.cpu_count())

    i = 0
    track_ids = []
    for index, track in unique_tracks_df.iterrows():
      i = i + 1

      track_ids.append(track['spotify_id'])

      if (i / 100).is_integer():
        pool.apply_async(retrieve_audio_features, args=(track_ids, spotify_acces_token), callback=collect_audio_features)
        
        track_ids = []
      elif len(unique_tracks_df) == i: 
        pool.apply_async(retrieve_audio_features, args=(track_ids, spotify_acces_token), callback=collect_audio_features)


    pool.close()
    pool.join()    

    print('length: ', str(len(unique_tracks_df)))

    

  except Exception as e:
    print('e: ', e)
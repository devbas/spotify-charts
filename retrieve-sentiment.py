import pandas as pd
import os
from glob import glob
import multiprocessing as mp
import sys 
import getopt 
import requests
import multiprocessing as mp
mp.set_start_method('spawn', True)

spotify_search_url = 'https://api.spotify.com/v1/search'
input_directory = ''

def auth_get(url, spotify_acces_token): 
  print('acces token: ', spotify_acces_token)
  result = requests.get(url, headers={"Authorization": "Bearer {}".format(spotify_acces_token)}).json()
  print('result: ', result) 
  return result

def retrieve_spotify_id(track_name, track_artist, spotify_acces_token): 
  try: 
    request_url = '{0}?v={1}%20{2}&type=track&limit=1&offset=0'.format(spotify_search_url, track_name, track_artist)

    s=auth_get(request_url, spotify_acces_token)
    print('response s: ', s)
    s.raise_for_status()

    response = s.content.decode('utf-8')

    return True

  except Exception as e:
    return False
    # print('sebs custom exception: ', e)


def main(spotify_acces_token):

  extension = '*.csv'
  
  try: 
    
    unique_tracks_df = pd.DataFrame(columns=['Track Name', 'Artist'])

    csv_files = [file for path, subdir, files in os.walk(input_directory) 
                for file in glob(os.path.join(input_directory, extension))]
  
    for file in csv_files:
      csv_df = pd.read_csv(file, header=0, usecols=['Track Name', 'Artist'])
      unique_csv_df = csv_df.drop_duplicates(subset=['Track Name', 'Artist'])
      unique_tracks_df = pd.concat([unique_tracks_df, unique_csv_df]).drop_duplicates().reset_index(drop=True)


    pool = mp.Pool(mp.cpu_count())
    print('start to apply')
    unique_tracks_df['spotify_id'] = [pool.apply_async(retrieve_spotify_id, 
      args=(track['Track Name'], track['Artist'], spotify_acces_token)) for index, track in unique_tracks_df.iterrows()]

    print('size: ', unique_tracks_df.size)

    pool.close()
    pool.join()    


  except Exception as e:
    print('e: ', e)

if __name__ == '__main__':

  spotify_acces_token = ''

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
      print('access token: ', spotify_acces_token)
    else: 
      print('retrieve-sentiment.py -d <csv directory> -t <spotify access token>')
      sys.exit()

  main(spotify_acces_token)
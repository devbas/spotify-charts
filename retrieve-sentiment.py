import pandas as pd
import os
from glob import glob
import multiprocessing as mp
import sys 
import getopt 

def main(argv):

  input_directory = ''
  extension = '*.csv'

  try: 
    opts, args = getopt.getopt(argv,"d:t",["directory","token"])
  except getopt.GetoptError as err:
    print('err: ', err)
    print('retrieve-sentiment.py -d <csv directory> -t <spotify access token>')
    sys.exit(2)
  
  for opt, arg in opts: 
    if opt in ('-d', '--directory'): 
      input_directory = arg
    else: 
      print('retrieve-sentiment.py -d <csv directory> -t <spotify access token>')
      sys.exit()
  
  try: 
    
    unique_tracks_df = pd.DataFrame(columns=['Track Name', 'Artist'])

    csv_files = [file for path, subdir, files in os.walk(input_directory) 
                for file in glob(os.path.join(input_directory, extension))]
  
    for file in csv_files:
      csv_df = pd.read_csv(file, header=0, usecols=['Track Name', 'Artist'])
      unique_csv_df = csv_df.drop_duplicates(subset=['Track Name', 'Artist'])
      unique_tracks_df = pd.concat([unique_tracks_df, unique_csv_df]).drop_duplicates().reset_index(drop=True)

    print('unique tracks_df: ', unique_tracks_df.size)

  except Exception as e:
    print('e: ', e)

if __name__ == '__main__':
  main(sys.argv[1:])
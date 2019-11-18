import pandas as pd
import requests
from io import StringIO
import multiprocessing as mp
from tqdm import tqdm
mp.set_start_method('spawn', True)

dates = pd.date_range(start='01/01/2017', end=pd.datetime.today()).to_pydatetime().tolist()
countries = ['us','gb','ad','ar','at','au','be','bg','bo','br','ca','ch','cl','co','cr','cy','cz','de','dk','do','ec','ee','es','fi','fr','gr','gt','hk','hn','hu','id','ie','il','in','is','it','jp','li','lt','lu','lv','mc','mt','mx','my','ni','nl','no','nz','pa','pe','ph','pl','pt','py','ro','se','sg','sk','sv','th','tr','tw','uy','vn','za']

def request_chart(url, date, country, header): 
    try: 
        s=requests.get(url)
        s.raise_for_status()
        
        response = s.content.decode('utf-8')
        
        if 'This chart does not exist. Please make another selection from the dropdown menus.' not in response:
            chart = pd.read_csv(StringIO(response), header=header, quotechar='"')
        
            chart['Date'] = date
            chart['Country'] = country 

            return chart
        
    except requests.exceptions.HTTPError as e:
        print('sebs custom exception: ', e)


def retrieve_charts(i, date, country, chart_type):

    try: 
        day = date.strftime('%Y-%m-%d')
    except ValueError:
        print('e: ')
    
    if chart_type == 'top': 
        chart = request_chart('https://spotifycharts.com/regional/' + country + '/daily/' + day + '/download', date, country, header=1)
        return chart
            
    if chart_type == 'viral': 
        chart = request_chart('https://spotifycharts.com/viral/' + country + '/daily/' + day + '/download', date, country, header=0)
        return chart


def collect_top_results(chart): 
    global total_top_df
    
    if isinstance(chart, tuple): 
        print('chart: ', chart) 
    else:
        total_top_df = total_top_df.append(chart)

def collect_viral_results(chart): 
    global total_viral_df
    total_viral_df = total_viral_df.append(chart)


if __name__ == '__main__':

    for i, country in enumerate(tqdm(countries)):

        pool = mp.Pool(mp.cpu_count())

        total_top_df = pd.DataFrame(columns=['Position', 'Track Name', 'Artist', 'Streams', 'URL', 'Date'])
        total_viral_df = pd.DataFrame(columns=['Position', 'Track Name', 'Artist', 'Streams', 'URL', 'Date'])

        for j, date in enumerate(dates):
            try: 
                pool.apply_async(retrieve_charts, args =(i, date, country, 'top'), callback=collect_top_results)
                pool.apply_async(retrieve_charts, args =(i, date, country, 'viral'), callback=collect_viral_results)
            except Exception as e: 
                print('e: ', e)

        pool.close()
        pool.join()

        total_top_df.to_csv('csv/total_top_{}.csv'.format(country))
        total_viral_df.to_csv('csv/total_viral_{}.csv'.format(country))
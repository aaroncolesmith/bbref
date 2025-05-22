
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

import random
import ast
import time


#set some options to view tables better
pd.set_option('display.max_colwidth',5000)
pd.options.display.max_columns = None
pd.set_option('display.max_rows', 1000)

import pyarrow as pa
import pyarrow.parquet as pq

from datetime import date, timedelta
from io import StringIO




def get_season(season):

  playoffs = False


  game_id = []
  game_url = []
  visitor_team = []
  home_team=[]
  visitor_score=[]
  home_score=[]

  months = ['October', 'November', 'December', 'January', 'February', 'March',
              'April', 'May', 'June','July']
  if season==2020:
      months = ['October-2019', 'November', 'December', 'January', 'February', 'March',
              'July', 'August', 'September', 'October-2020']
  df = pd.DataFrame()
  for month in months:
      r = requests.get(f'https://www.basketball-reference.com/leagues/NBA_{season}_games-{month.lower()}.html')
      if r.status_code==200:
          soup = BeautifulSoup(r.content, 'html.parser')
          table = soup.find('table', attrs={'id': 'schedule'})
          if table:
              month_df = pd.read_html(StringIO(str(table)))[0]
              # game_id = []
              # game_url = []
              for row in table.find_all('tr'):
                try:
                  if 'csk' in str(row):
                    game_id.append(str(row).split('csk="')[1].split('"')[0])
                    game_url.append(str(row).split('data-stat="box_score_text"><a href="')[1].split('"')[0])
                    visitor_team.append(str(row).split('data-stat="visitor_team_name"><a href="/teams/')[1].split('/')[0])
                    home_team.append(str(row).split('data-stat="home_team_name"><a href="/teams/')[1].split('/')[0])
                    visitor_score.append(str(row).split('data-stat="visitor_pts">')[1].split('<')[0])
                    home_score.append(str(row).split('data-stat="home_pts">')[1].split('<')[0])
                except:
                  pass
              df = pd.concat([df, month_df])

  df = df.reset_index()

  cols_to_remove = [i for i in df.columns if 'Unnamed' in i]
  # cols_to_remove += [i for i in df.columns if 'Notes' in i]
  cols_to_remove += [i for i in df.columns if 'Start' in i]
  cols_to_remove += [i for i in df.columns if 'Attend' in i]
  cols_to_remove += [i for i in df.columns if 'Arena' in i]
  cols_to_remove += [i for i in df.columns if 'LOG' in i]
  cols_to_remove += ['index']
  df = df.drop(cols_to_remove, axis=1)
  df.columns = ['DATE', 'VISITOR', 'VISITOR_PTS', 'HOME', 'HOME_PTS','NOTES']

  if season==2020:
      df = df[df['DATE']!='Playoffs']
      df['DATE'] = df['DATE'].apply(lambda x: pd.to_datetime(x))
      df = df.sort_values(by='DATE')
      df = df.reset_index().drop('index', axis=1)
      playoff_loc = df[df['DATE']==pd.to_datetime('2020-08-17')].head(n=1)
      if len(playoff_loc.index)>0:
          playoff_index = playoff_loc.index[0]
      else:
          playoff_index = len(df)

      playoff_date = df.iloc[playoff_index+1].DATE
      if playoffs:
          df = df[playoff_index:]
      else:
          df = df[:playoff_index]
  else:
      if season == 1953:
          df.drop_duplicates(subset=['DATE', 'HOME', 'VISITOR'], inplace=True)
      playoff_loc = df[df['DATE']=='Playoffs']
      playoff_loc = df[df['NOTES']=='Play-In Game']
      if len(playoff_loc.index)>0:
          playoff_index = playoff_loc.index[0]
      else:
          playoff_index = len(df)
      if season == 1980:
        playoff_date = pd.to_datetime('1980-04-02')
      if season == 2021:
        playoff_date = pd.to_datetime('2021-05-17')
      else:
        try:
          playoff_date = df.iloc[playoff_index+1].DATE
        except:
          playoff_date=pd.to_datetime(df.DATE).max()+timedelta(days=1)
      if playoffs:
        df = df[playoff_index+1:]
      else:
          df = df[:playoff_index]
      df['DATE'] = df['DATE'].apply(lambda x: pd.to_datetime(x))

  df['game_id'] = pd.Series(game_id)
  df['game_url'] = pd.Series(game_url)


  d2 = pd.DataFrame(
                  {'game_id':game_id[:len(game_url)],
                  'game_url':game_url,
                  'visitor_team':visitor_team,
                  'visitor_score':visitor_score,
                  'home_team':home_team,
                  'home_score':home_score,
                  'season':season
                  })
  d2['date'] = pd.to_datetime(d2['game_id'].str[:8])
  d2.loc[d2.date >= playoff_date, 'game_type'] = 'Playoffs'
  d2.loc[d2.date < playoff_date, 'game_type'] = 'Regular Season'

  return d2


def get_box_score(game_id, visiting_team, home_team):
  proxy= get_proxy()
  url = 'https://www.basketball-reference.com/boxscores/'+game_id+'.html'
  # print(url)

  #sleep for a random interval of 3 - 9
  sleep_time=2.0 + np.random.uniform(1,6) +  np.random.uniform(0,1)
  # print('sleeping for :'+str(sleep_time))
  time.sleep(sleep_time )

  df=pd.DataFrame()

  r=requests.get(url,proxies=proxy)

  if r.status_code == 429:
    hold_for = r.headers.get('retry-after')
    print('Holding on initial')
    print(hold_for)
    time.sleep(hold_for+10)
    r=requests.get(url,proxies=proxy,headers=headers)

  d = pd.read_html(r.content)
  # d=pd.read_html(url)

  for i in range(len(d)):
    try:
      d[i].columns = d[i].columns.droplevel()
    except:
      pass

  ## creating dataframe and adding all values for visiting team
  df=pd.concat([df,pd.merge(d[0],d[int((len(d)/2)-1)])],sort=False)

  ## add in the visiting team name
  df['team'] = visiting_team

  ### This was the old version -- not totally sure why the try existed
  # try:
  #   df=pd.concat([df,pd.merge(d[0],d[int((len(d)/2)-1)])],sort=False)
  #   print('first try failed')
  # except:
  #   df=pd.merge(d[0],d[int((len(d)/2)-1)])

  ## adding data for the home team
  df = pd.concat([df,pd.merge(d[int((len(d)/2))],d[int((len(d))-1)])],sort=False)
  df.loc[df.team.isnull(), 'team'] = home_team

  try:
    df=df.loc[df.Starters != 'Team Totals']
    df=df.loc[df.Starters != 'Reserves']
  except:
    df=df.loc[df.Player != 'Team Totals']
    df=df.loc[df.Player != 'Reserves']

  df=df.loc[df.MP != 'Did Not Play']
  df=df.loc[df.MP != 'Did Not Dress']
  df=df.loc[df.MP != 'Not With Team']
  df=df.loc[df.MP != 'DNP']
  df=df.reset_index(drop=True)

  #If there are actual values for Minutes Played, convert to an integeer
  if df[['MP']].isnull().all()['MP'] != True:
    try:
      df['MP']=round(pd.to_timedelta('00:' + df['MP']).astype('timedelta64[s]').astype(int) / 60,2)
    except:
      df['MP']=df.MP.str.replace(':','.')

  df['game_id'] = url[-17:].replace('.html','')

  df.rename(columns={'Starters':'player'}, inplace=True)
  df['date']=pd.to_datetime(df['game_id'].str[:8])

  df.columns = [x.lower() for x in df.columns]
  df.columns = [x.replace('%','_pct') for x in df.columns]

  return df


def get_proxy():
  r=requests.get('https://www.us-proxy.org/')
  soup = BeautifulSoup(r.content, 'html.parser')
  table = soup.find_all('table')[0]
  df = pd.read_html(str(table))[0]
  i=random.randint(0,df.index.size-3)
  proxy_string = "{'http': '"+ df.loc[i]['IP Address']+':'+df.loc[i]['Port'].astype('str')+"'}"
  proxy = ast.literal_eval(proxy_string)
  return proxy



## Load Data
d1=pd.read_parquet('/content/drive/MyDrive/Analytics/nba_games.parquet', engine='pyarrow')
## previous version, before merging w/ the other scraped data
# d2=pd.read_parquet('/content/drive/MyDrive/Analytics/nba_box_scores.parquet', engine='pyarrow')
## updated version w/ data from the other scraped data
d2=pd.read_parquet('/content/drive/MyDrive/Analytics/data_nba_games_20230422.parquet',engine='pyarrow')
try:
  del d2['unnamed: 16_level_1']
except Exception as e:
  print(e)



df=get_season(2025)

d1=pd.concat([d1,df]).reset_index(drop=True)
d1['visitor_score'] = pd.to_numeric(d1['visitor_score'])
d1['home_score'] = pd.to_numeric(d1['home_score'])
d1 = d1.drop_duplicates()

table = pa.Table.from_pandas(d1)
# Parquet with Brotli compression
pq.write_table(table, '/content/drive/MyDrive/Analytics/nba_games.parquet',compression='BROTLI')

box_scores = pd.DataFrame()
print(d2.index.size)
## first one is where need to add more games
for i,r in pd.merge(d1,
         d2.groupby(['game_id']).size().to_frame('cnt').reset_index(),
         how='left').query('cnt.isnull()').iterrows():
         d=get_box_score(r['game_id'], r['visitor_team'], r['home_team'])
         box_scores = pd.concat([box_scores,d])

d2=pd.concat([d2,box_scores])

num_cols = ['mp', 'fg', 'fga', 'fg_pct', '3p', '3pa', '3p_pct', 'ft',
       'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf',
       'pts', '+/-', 'ts_pct', 'efg_pct', '3par', 'ftr', 'orb_pct', 'drb_pct',
       'trb_pct', 'ast_pct', 'stl_pct', 'blk_pct', 'tov_pct', 'usg_pct',
       'ortg', 'drtg', 'bpm']
d2[num_cols] = d2[num_cols].apply(pd.to_numeric, errors='coerce')

print(d2.index.size)

table = pa.Table.from_pandas(d2)
pq.write_table(table, '/content/drive/MyDrive/Analytics/data_nba_games_20230422.parquet',compression='BROTLI')



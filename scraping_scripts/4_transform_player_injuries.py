# ----------------------------- Load Packages -----------------------------
import pandas as pd
import numpy as np
from datetime import date

from scraping_scripts.Functions.function_clean_date import clean_date

# Access to Data Lake and Data Warehouse
import psycopg2
from scraping_scripts.Credentials import data_lake

# ----------------------------- Connect to Data Lake -----------------------------
# Data Lake Credentials
creds = data_lake()

# Connect to Data Lake
conn = psycopg2.connect(host = creds["host"],
                        database = creds["database"],
                        user = creds["user"],
                        password = creds["password"])

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# Auto commit
conn.set_session(autocommit=True)

# Execute SQL statment
cur.execute("SELECT * FROM historical_injuries;")

# Fetch all data from table
tuples_list = cur.fetchall()

# Column names for the DataFrame
column_names = ['injury_id','transfermarkt_id', 'player','dob','height',
                'nationality','int_caps','int_goals','current_club',
                'shirt_number','season','injury','date_from','date_until',
                'days','games_missed','scrape_time']

# Create DataFrame
injuries_df_1 = pd.DataFrame(tuples_list, columns=column_names)

# Close session
cur.close()
conn.close()

# ----------------------------- Transform DataFrame -----------------------------
# Save new instance of DataFrame
injuries_df_2 = injuries_df_1

# Drop columns not needed for Data Warehouse
injuries_df_2 = injuries_df_2.drop(['injury_id', 'transfermarkt_id', 'shirt_number', 'scrape_time'], axis = 1)

# Replace the empty strings and '-'
injuries_df_2 = injuries_df_2.replace(['NA'], np.nan)
injuries_df_2['date_until'] = injuries_df_2['date_until'].replace(['-'], np.nan)
injuries_df_2['games_missed'] = injuries_df_2['games_missed'].replace(['?','-'], "0").astype('float')
injuries_df_2[['int_caps', 'int_goals']] = injuries_df_2[['int_caps', 'int_goals']].fillna('0')

# ----------------------------- Player Names -----------------------------

# Strip all leading and trailing whitespace from player names
injuries_df_2['player'] = injuries_df_2.player.str.strip()

# Split name into first and second name
injuries_df_2[['first_name', 'second_name']] = injuries_df_2['player'].str.split(' ', n = 1, expand = True)

# Remove players with no first name
injuries_df_2 = injuries_df_2[~injuries_df_2['first_name'].isnull()]

# Drop player column
injuries_df_2 = injuries_df_2.drop(['player'], axis = 1)

# ----------------------------- Date Columns -----------------------------

# Standarise format of all date columns to ('mmm dd, yyy')
injuries_df_2['dob'] = [v[:-4] + " " + v[-4:] for v in injuries_df_2['dob']]
injuries_df_2['dob'] = [v[:3] + " " + v[3:] for v in injuries_df_2['dob']]

# Clean dob column
clean_date(injuries_df_2, 'dob')

# Create Age column: Days difference
injuries_df_2['age'] = date.today() - injuries_df_2['dob']

# Create Age column: Convert age to years
injuries_df_2['age'] =  round(injuries_df_2['age'] / np.timedelta64(1, 'Y'),0)

# Clean date_from column
clean_date(injuries_df_2, 'date_from')

# Clean date_until column
clean_date(injuries_df_2, 'date_until')

# ----------------------------- Clubs -----------------------------

# Create clubs dictionary
club_dict = {'Arsenal FC':'Arsenal', 'Aston Villa':'Aston Villa',
             'AFC Bournemouth': 'Bournemouth','Brentford FC': 'Brentford',
             'Brighton & Hove Albion U21': 'Brighton','Brighton & Hove Albion': 'Brighton',
             'Chelsea FC': 'Chelsea', 'Crystal Palace': 'Crystal Palace',
             'Everton FC': 'Everton', 'Fulham FC': 'Fulham',
             'Leicester City': 'Leicester', 'Leeds United U21':'Leeds',
             'Leeds United':'Leeds', 'Liverpool FC':'Liverpool',
             'Manchester City': 'Man City','Manchester United':'Man Utd',
             'Newcastle United':'Newcastle', 'Nottingham Forest':'Nott\'m Forest',
             'Southampton FC':'Southampton', 'Tottenham Hotspur':'Spurs',
             'West Ham United': 'West Ham', 'Wolverhampton Wanderers':'Wolves'}

# Change names of clubs
injuries_df_2['current_club'] = injuries_df_2['current_club'].map(club_dict)

# ----------------------------- Seasons -----------------------------
# Create seasons dictionary
season_dict = {'22/23':'2022/23', '21/22':'2021/22', '20/21':'2020/21',
               '19/20':'2019/20', '18/19':'2018/19', '17/18':'2017/18',
               '16/17':'2016/17', '15/16':'2015/16', '14/15':'2014/15',
               '13/14':'2013/14', '12/13':'2012/13', '11/12':'2011/12',
               '10/11':'2010/11', '09/10':'2009/10', '08/09':'2008/09',
               '07/08':'2007/08', '06/07':'2006/07', '05/06':'2005/06',
               '04/05':'2004/05', '03/04':'2003/04', '02/03':'2002/03'}

# Change names of seasons
injuries_df_2['season'] = injuries_df_2['season'].map(season_dict)

# ----------------------------- Days Injured -----------------------------
# Clean days injured
injuries_df_2.rename({'days': 'days_injured'}, axis=1, inplace=True)
injuries_df_2['days_injured'] = injuries_df_2['days_injured'].str.rstrip(' days').astype('float')

# Re order columns
injuries_df_2 = injuries_df_2[['first_name', 'second_name','current_club',
                               'dob_day', 'dob_mon', 'dob_year', 'dob',
                               'age', 'height', 'nationality', 'int_caps',
                               'int_goals', 'season', 'injury', 'date_from_day',
                               'date_from_mon', 'date_from_year', 'date_from',
                               'date_until_day', 'date_until_mon', 'date_until_year',
                               'date_until', 'days_injured', 'games_missed']]

# ----------------------------- Create Bios DataFrame -----------------------------

# Create player bios DataFrame
player_bios = injuries_df_2[['first_name', 'second_name','current_club',
                             'dob_day', 'dob_mon','dob_year', 'dob','age',
                             'height', 'nationality','int_caps', 'int_goals']]

# Remove duplicate rows
player_bios = player_bios.drop_duplicates()

# Change types
player_bios = player_bios.astype({'dob_day': 'int', 'dob_mon': 'int', 'dob_year': 'int',
                                  'age': 'int', 'height': 'int', 'int_caps': 'int',
                                  'int_goals': 'int'})

# ----------------------------- Create Player Injuries DataFrame -----------------------------
# Create player injuries DataFrame
player_injuries = injuries_df_2[['first_name', 'second_name','current_club',
                                 'season', 'injury', 'date_from_day',
                                 'date_from_mon', 'date_from_year', 'date_from',
                                 'date_until_day', 'date_until_mon', 'date_until_year',
                                 'date_until', 'days_injured', 'games_missed']]

# Remove players with no injury history
player_injuries = player_injuries[player_injuries['season'].notna()]

# Change types
player_injuries = player_injuries.astype({'date_from_day': 'int', 'date_from_mon': 'int', 'date_from_year': 'int',
                                          'days_injured': 'int', 'games_missed': 'int'})

# Extract current injuries
current_injuries = player_injuries[player_injuries['date_until'].isna()]

# Drop columns not needed for Data Warehouse
current_injuries = current_injuries.drop(['date_until_day', 'date_until_mon',
                                          'date_until_year', 'date_until'], axis = 1)

# Remove current injuries
historical_injuries = player_injuries[player_injuries['date_until'].notna()]

# Change types
historical_injuries = historical_injuries.astype({'date_until_day': 'int', 'date_until_mon': 'int',
                                                  'date_until_year': 'int'})

# ----------------------------- Export to .csv -----------------------------
# Player historical injuries
pd.DataFrame.to_csv(historical_injuries, "historical_injuries.csv", index = False)
print("Export Complete")

# Player current injuries
pd.DataFrame.to_csv(current_injuries, "current_injuries.csv", index = False)
print("Export Complete")

# Player bios
pd.DataFrame.to_csv(player_bios, "player_bios.csv", index = False)
print("Export Complete")
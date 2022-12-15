# ----------------------------- Load Packages -----------------------------
# For Scraping
import requests
from bs4 import BeautifulSoup

# Functions for Scraping
from scraping_scripts.Functions.function_player_bios import player_bio
from scraping_scripts.Functions.function_player_stats import table_data

# To load to Data Lake
import psycopg2
from scraping_scripts.Credentials import data_lake

# ----------------------------- Connect to Data Lake  -----------------------------
# Empty list for team URLs
player_urls = []

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

# Auto commit is very important
conn.set_session(autocommit=True)

# Find team URLs in Data Lake
cur.execute('''SELECT player_url FROM player_urls''')

# Extract player URLs from Data Lake
for row in cur.fetchall():
    player_urls.append(row[0])

# Close session
cur.close()
conn.close()

# ----------------------------- Scrape players injuries  -----------------------------
# Empty lists to insert player data
## NB stats1 is for player with one injury page and stats2 for players with multiple injury pages
stats1 = []
stats2 = []

# Counter for progress of scraping
counter = 0

# Headers required to scrape Transfermarkt
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

# Loop through each player url
for url in player_urls:

    # ----------------------------- Obtain html data -----------------------------
    # Get content of url
    source = requests.get(url, headers=headers)

    # Parse html into BS4 object
    soup = BeautifulSoup(source.content, 'html.parser')

    # Search for number of injury pages
    test_pages = soup.find_all('ul', attrs={'class': "tm-pagination"})

    # ----------------------------- Scrape players with x1 injury page -----------------------------
    # If player has one injury page
    if len(test_pages) == 0:

        # Get player biography
        player_bios = player_bio(soup)

        # Add injury history to player_biography
        player_injury_history = table_data(soup, player_bios)

        # Table data
        dataframe_stats_1 = player_injury_history[0]

        # Table headers
        dataframe_headers = player_injury_history[1]

        # Extend table data with previous players in the loop
        stats1.extend(dataframe_stats_1)

    # ----------------------------- Scrape players with multiple injury pages -----------------------------

    # If player has multiple injury pages
    else:
        # Get url's for each injury table
        for div in test_pages:
            info = div.find_all('a')
            num_pages = len(info) - 2

            # Iterate through the different injury pages
            while num_pages > 0:

                # Create new urls for each injury page
                new_url = str(url) + "/page/" + str(num_pages)

                # Get content of url
                source = requests.get(new_url, headers=headers)

                # Parse html into BS4 object
                soup = BeautifulSoup(source.content, 'html.parser')

                # Get player biography
                player_bios = player_bio(soup)

                # Add injury history to player_biography
                player_injury_history = table_data(soup, player_bios)

                # Table data
                dataframe_stats_2 = player_injury_history[0]

                # Table headers
                dataframe_headers = player_injury_history[1]

                # Extend table data with previous players in the loop
                stats2.extend(dataframe_stats_2)

                # Scrape next injury page for player
                num_pages = num_pages - 1

    # Print progress of scraping
    counter += 1
    if (counter % 50) == 0:
        print(str(counter) + " Players Scraped")

print("Successfully Scraped: " + str(counter) + " Players")

# ----------------------------- Connect to Data Lake -----------------------------

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

# Auto commit is very important
conn.set_session(autocommit=True)

# ----------------------------- Drop and create table in Data Lake -----------------------------
try:
    cur.execute("DROP TABLE historical_injuries;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# ----------------------------- Create new Historical_Injuries Table -----------------------------
try:
    cur.execute("CREATE TABLE IF NOT EXISTS historical_injuries (injury_id SERIAL NOT NULL, transfermarkt_id VARCHAR(255), player VARCHAR(255),\
                dob VARCHAR(255), height VARCHAR(255), nationality VARCHAR(255), int_caps VARCHAR(255),\
                int_goals VARCHAR(255), current_club VARCHAR(255), shirt_number VARCHAR(255), season VARCHAR(255), injury VARCHAR(255),\
                date_from VARCHAR(255), date_until VARCHAR(255), days VARCHAR(255), games_missed VARCHAR(255));")
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# ----------------------------- Add data to table in Data Lake -----------------------------
# Create Query
mySql_insert_query = """INSERT INTO historical_injuries (transfermarkt_id, player, dob, height, nationality, \n
                                                        int_caps, int_goals, current_club, shirt_number, season, \n
                                                        injury, date_from, date_until, days, games_missed) 
                           VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s) """

# Join injury and bio data for all players
stats_joined = stats1 + stats2

# Insert data into Data Lake
cur.executemany(mySql_insert_query, stats_joined)
conn.commit()
print(cur.rowcount, "Records inserted successfully into table")

# Close session
cur.close()
conn.close()



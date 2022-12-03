# ----------------------------- Load Packages -----------------------------
# For Scraping
import requests
from bs4 import BeautifulSoup

# To load to Data Lake
import psycopg2
from scraping_scripts.Credentials import data_lake

# ----------------------------- Get team URLs from Data Lake -----------------------------

# Empty list for team URLs
team_urls = []

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
cur.execute('''SELECT team_url FROM team_urls''')

# Extract team URLs from Data Lake
for row in cur.fetchall():
    team_urls.append(row[0])

# Close session
cur.close()
conn.close()

# ----------------------------- Scrape player URLs -----------------------------

# Empty lists to add player names and urls
player_name = []
player_url = []
player_counter = 0

# Headers required to scrape Transfermarkt
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

for i in team_urls:
    # Download content of url
    source = requests.get(i, headers = headers)

    # Parse html into BS4 object
    soup = BeautifulSoup(source.content, 'html.parser')

    # Find and extract team urls
    for div in soup.find_all('span', attrs={'class': "show-for-small"}):
        info = div.find_all('a')
        for a in info:
            # Get player name
            n = 2
            player_name.append(str(a.contents)[n:-n])

            # Get player url and change to injury section
            s = str(a['href'].rsplit("/", 3)[0])
            t = "/verletzungen/"
            u = str(a['href'].split("/", 3)[3])
            player_url.append("https://www.transfermarkt.com" + s + t + u)

        player_counter += 1
        if (player_counter % 50) == 0:
            print(str(player_counter) + " Player URLs Scraped")

print("Scraping Completed: " + str(len(player_url)) + " Player URLs obtained")

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

# Drop old table
try:
    cur.execute("DROP TABLE player_urls;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create new table
try:
    #cur.execute(sqlCreateTable)
    cur.execute("CREATE TABLE IF NOT EXISTS player_urls (player_url_id SERIAL NOT NULL, player_name VARCHAR(255), player_url VARCHAR(255));")
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# ----------------------------- Add data to table in Data Lake -----------------------------

# Create format for load
sql = 'INSERT INTO player_urls (player_name, player_url) VALUES (%s, %s)'

# Add data to table
for elem in zip(player_name, player_url):
    cur.execute(sql, elem)
    conn.commit()

print('Player URLs successfully loaded to Data Lake')

# Close session
cur.close()
conn.close()


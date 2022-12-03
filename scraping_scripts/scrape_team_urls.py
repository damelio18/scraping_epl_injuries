# ----------------------------- Load Packages -----------------------------
# For Scraping
import requests
from bs4 import BeautifulSoup

# To load to Data Lake
import psycopg2
from scraping_scripts.Credentials import data_lake
# ----------------------------- Get Team URLs -----------------------------

# Empty lists to add team names and urls
team_name = []
team_url = []

# Headers required to scrape Transfermarkt
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

# Url to scrape
url = "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"

# Download content of url
source = requests.get(url, headers = headers)

# Parse html into BS4 object
soup = BeautifulSoup(source.content, 'html.parser')

# Find team urls and names
for div in soup.findAll('td', attrs={'class': "hauptlink no-border-links"}):
    info = div.find_all('a')
    for a in info:

        # Extract team names
        substring = '.png'
        if substring in str(a.contents):
            pass
        else:
            n = 2
            team_name.append(str(a.contents)[n:-n])

        # Extract team urls
        if str(a['href']).rsplit("/", 2)[0] == "#":
            pass
        else:
            team_url.append("https://www.transfermarkt.com" + str(a['href']).rsplit("/", 2)[0])

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

# Auto commit is very important
conn.set_session(autocommit=True)

# ----------------------------- Drop and Create New Table -----------------------------

# Drop old table
try:
    cur.execute("DROP TABLE Team_URLs;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create new table
try:
    #cur.execute(sqlCreateTable)
    cur.execute("CREATE TABLE IF NOT EXISTS Team_URLs (team_id SERIAL NOT NULL, team_name VARCHAR(255), team_url VARCHAR(255));")
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# ----------------------------- Add data to table in Data Lake -----------------------------

# Create format for load
sql = 'INSERT INTO Team_URLs (team_name, team_url) VALUES (%s, %s)'

# Add data to table
for elem in zip(team_name, team_url):
    cur.execute(sql, elem)
    conn.commit()

print("Records inserted successfully into table")

# Close session
cur.close()
conn.close()
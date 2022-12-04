# Function to get player injury history data
def table_data(x, y):

    '''
    Argument:
    y = bs4 object with parsed html of the webpage
    x = list containing biography information, with 7 elements

    Returns:
    stats = a list of all table data for player injuries
    [headers] = a list of headers for columns
        NB. both stats and [headers] can be used to join as a dataframe
    '''

    # Change name of html page input into variable
    soup = x
    bio = y

    # ----------------------------- For players with no injury data -----------------------------

    # Default values in case a player doesn't have injury data on their webpage
    na_injuries = ["NA", "NA", "NA", "NA", "NA", "NA"]

    # Insert biographical data into na_injuries
    counter = 0
    for i in bio:
        na_injuries.insert(counter, i)
        counter += 1

    # Create list within list to match format of players with injury data
    table_data = [na_injuries]

    # Headers for players without injury data
    headers = ["Transfermarkt_ID", "Player", "DOB", "Height", "Nationality", "International_Caps",
               "International_Goals", "Current_Club", "Shirt_Number", "Season", "Injury", "From", "Until",
               "Days", "Games missed"]

    # ----------------------------- For players with injury data -----------------------------

    try:
        # Get table
        table = soup.find('table')

        # Find table headers
        all_headers = table.findAll("th")

        # Extract headers
        headers = [h.text.strip() for h in all_headers[1:]]

        # Ensure it is the injuries table
        if len(headers) > 4:

            # Add columns to the header list
            headers.insert(0, "Transfermarkt_ID")
            headers.insert(1, "Player")
            headers.insert(2, "DOB")
            headers.insert(3, "Height")
            headers.insert(4, "Nationality")
            headers.insert(5, "International_Caps")
            headers.insert(6, "International_Goals")
            headers.insert(7, "Current_Club")
            headers.insert(8, "Shirt_Number")
            headers.insert(9, "Season")

            # Find table rows
            rows = table.findAll("tr")[1:]

            # Extract table rows
            table_data = [[td.getText() for td in rows[i].findAll('td')] for i in range(len(rows))]

            # Insert biographical data to injury data
            for l in range(0, len(table_data)):
                table_data[l].insert(0, bio[0])
                table_data[l].insert(1, bio[1])
                table_data[l].insert(2, bio[2])
                table_data[l].insert(3, bio[3])
                table_data[l].insert(4, bio[4])
                table_data[l].insert(5, bio[5])
                table_data[l].insert(6, bio[6])
                table_data[l].insert(7, bio[7])
                table_data[l].insert(8, bio[8])

        else:
            pass

    except:
        pass

    # Add player data as lists within a list
    stats = []
    stats.extend(table_data)

    return stats, [headers]



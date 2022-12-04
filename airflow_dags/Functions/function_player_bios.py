import string

# ----------------------------- Function to get player biographies (metadata) -----------------------------
def player_bio(y):
    '''
    Argument:
    y = bs4 object with parsed html of the webpage

    Returns:
    A list of player biography stats with 9 elements
    '''

    # change name of html page input into variable
    soup = y

    # Default values in case a player doesn't have a piece of biographical data on their webpage
    player_id = "NA"
    player = "NA"
    DOB = "NA"
    height = "NA"
    nationality = "NA"
    int_caps = "NA"
    int_goals = "NA"
    club = "NA"
    shirt_no = "NA"

    # Get Transfermarkt player ID
    try:
        for div in soup.findAll('div', attrs={'class': "data-header__badge-container"}):
            player_id = div.find_all('a')
            player_id = str(player_id[0]['href']).rsplit("/", 1)[1]
    except:
        pass

    # Get player name
    try:
        for div in soup.findAll('h1', attrs={'class': "data-header__headline-wrapper"}):
            full_name = div.contents
            first = full_name[2].translate({ord(c): None for c in string.whitespace}).ljust(1, ' ')
            second = str(full_name[3].contents).replace("[", "").replace("]", "")[1:-1]
            player = first + " " + second
    except:
        pass

    # Get player date of birth
    try:
        for div in soup.findAll('span', attrs={'itemprop': "birthDate"}):
            DOB = div.contents
            DOB = DOB[0].translate({ord(c): None for c in string.whitespace})[:-4]
    except:
        pass

    # Get player height
    try:
        for div in soup.findAll('span', attrs={'itemprop': "height"}):
            height = str(div.contents)[2:-4].replace(",", "")
    except:
        pass

    # Get player nationality
    try:
        for div in soup.findAll('span', attrs={'class': "data-header__content"}):
            nation = div.find_all('img')
            # Get last nationality from flags on webpage
            for a in nation:
                nationality = str(a['title'])
    except:
        pass

    # Get number of player's international caps and goals
    try:
        counter_int = 0
        for div in soup.findAll('a', attrs={'class': "data-header__content data-header__content--highlight"}):
            # Counter to determine if a player has caps and goals info available
            counter_int += 1
            if counter_int == 1:
                int_caps = str(div.contents).translate({ord(c): None for c in string.whitespace})[2:-2]
            else:
                int_goals = str(div.contents).translate({ord(c): None for c in string.whitespace})[2:-2]
    except:
        pass

    # Get player's current club
    try:
        for div in soup.findAll('span', attrs={'class': "data-header__club"}):
            club_search = div.find_all('a')
            for a in club_search:
                club = str(a['title'])
    except:
        pass

    # Get player's shirt number
    try:
        shirt_no = str(name_all[1].contents).translate({ord(c): None for c in string.whitespace}).replace("'\\n#", "")[
                   1:-2]
    except:
        pass

    # Output from function is list of bigraphical data (9 elements)
    return [player_id, player, DOB, height, nationality, int_caps, int_goals, club, shirt_no]

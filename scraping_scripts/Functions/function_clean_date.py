# ----------------------------- Function to clean date format -----------------------------

def clean_date(x, y):
    '''

    Argument:
    x = DataFrame object
    y = Name of column in DataFrame with date in string format 'mmm dd, yyyy'

    Returns:
    DataFrame with date column in datetime format and columns in float type for day, month and year
    '''

    df = x
    df_col = y
    col_day = (y + "_day")
    col_mon = (y + "_mon")
    col_year = (y + "_year")

    # Create months dictionary
    month_dict = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                  'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

    # Extract day
    df[col_day] = df[df_col].str[4:6]
    df[col_day] = df[col_day].replace(",", "", regex = True).astype('float')

    # Extract Month and convert to number (Jan = 1, Feb = 2 etc.)
    df[col_mon] = df[df_col].str[:3]
    df[col_mon] = df[col_mon].map(month_dict).astype('float')

    # Extract Year
    df[col_year] = df[df_col].str[-4:].astype('float')

    # Create dob
    df[df_col] = pd.to_datetime(dict(year = df[col_year], month=df[col_mon], day=df[col_day]))

    return df
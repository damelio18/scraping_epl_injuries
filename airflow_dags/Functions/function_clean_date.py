# ----------------------------- Function to clean date format -----------------------------
import pandas as pd

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
    try:
        df[col_day] = df[df_col].str[4:6]
        df[col_day] = df[col_day].str.replace(",", "", regex = True)
    except:
        pass
    #df[col_day] = df[col_day].str.replace(",", "")

    try:
        # Extract Month and convert to number (Jan = 1, Feb = 2 etc.)
        df[col_mon] = df[df_col].str[:3]
        df[col_mon] = df[col_mon].map(month_dict).astype('float')
    except:
        pass

    try:
        # Extract Year
        df[col_year] = df[df_col].str[-4:].astype('float')
    except:
        pass

    try:
        # Create dob
        df[df_col] = pd.to_datetime(dict(year = df[col_year], month=df[col_mon], day=df[col_day]))
    except:
        pass

    try:
        #Remove time from format
        df[df_col] = pd.to_datetime(df[df_col]).dt.date
    except:
        pass

    return df


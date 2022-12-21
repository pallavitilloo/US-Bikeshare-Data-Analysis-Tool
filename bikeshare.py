import pandas as pd
import numpy as np
from datetime import datetime

# Defining the dictionaries for mapping
city_dict = {'chicago':'chicago.csv', 'new york': 'new_york_city.csv', 'washington dc':'washington.csv'}
month_dict = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6}
day_dict = {'mon':0, 'tue':1, 'wed':2, 'thu':3, 'fri':4, 'sat':5, 'sun':6}
month_names = {1: 'January', 2: 'February', 3:'March', 4: 'April', 5:'May', 6:'June'}
day_names = {0: 'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}

# Raw data will be shown in 5 lines at a time. This value can be changed to show more or less data rows at a time
raw_data_batch_size = 5
day = month = 'all'
continue_flag = True
valid_choice = True
accepted_yeses = ['yes','y']
max_screen_width = 175

# Function definitions
def load_data(city, month, day):
    '''
    This function loads data for the specified city for the specified month and/or day
    :param city: (string) The city for which data needs to be fetched. This will be mapped into city_dict to fetch that CSV file
    :param month: (string) The month for which user would like to filter the data
    :param day: (string) The day (weekday) for which the user would like to filter the data
    :return: (DataFrame) Returns the loaded data into a Pandas DataFrame
    '''
    file_name = city_dict.get(city.lower())

    # Here parse_dates will ensure that the specified columns will be read as datetime columns
    city_data = pd.read_csv(file_name, parse_dates=['Start Time', 'End Time'])

    # Create additional columns which will be used to display statistical information to the user
    city_data['Month'] = city_data['Start Time'].dt.month
    city_data['Day'] = city_data['Start Time'].dt.dayofweek
    city_data['Hour'] = city_data['Start Time'].dt.hour
    city_data['Trip'] = city_data['Start Station'] + ' TO ' + city_data['End Station']

    if month != 'all' and month in month_dict:
        city_data = city_data[city_data['Month'] == month_dict.get(month)]

    if day != 'all' and day in day_dict:
        city_data = city_data[city_data['Day'] == day_dict.get(day)]

    return city_data

def get_popular_data(city_data, parameter):
    '''
    This function fetches the 'mode' i.e. the most common value for the specified column in the dataframe
    :param city_data: (DataFrame) The dataframe containing all data
    :param parameter: (string) Column name for which the mode is to be computed
    :return: popular, count_popular: Value of the mode and count of its value found in the column
    '''
    popular = city_data[parameter].mode()[0]
    count_popular = city_data[parameter][df_data[parameter] == popular].count()
    return popular, count_popular

def get_data_in_batches(city_data, batch_size, column_list):
    '''
    This is a generator function that yields a number of rows at a time from the dataframe
    :param city_data: (DataFrame) The dataframe from which data needs to be fetched
    :param batch_size: (int) The batch size for one set of rows
    :param column_list: (list) List of columns to be fetched
    :return: yields a set of rows from the dataframe as specified in the batch_size
    '''
    for i in range(0, len(city_data), batch_size):
        yield city_data[i:i+batch_size][column_list]

def print_heading(heading):
    '''
    Prints the given string in a heading format on the screen
    :param heading: The heading to be printed
    :return: None
    '''
    no_of_stars = (max_screen_width - len(heading))//2
    print(("\n" + "*" * no_of_stars + "  {}  " + "*" * no_of_stars).format(heading))

# Program execution begins here
print_heading("US BIKESHARE DATA INTERACTIVE TOOL")

try:
    while continue_flag:
        try:
            valid_choice = True
            choice = input('\nWhich city data would you like to see ? (Chicago/New York/Washington DC) : ').lower()
            if choice not in city_dict:
                print('Invalid input!\n')
                valid_choice = False
            else:
                filter = input('Would you like to put any filters on the data ? (Month/Day/Both/None) : ').lower()
                if filter not in ['month','day','none','both']:
                    valid_choice = False
                else:
                    # If no filter needs to be applied, we will fetch all the data from the CSV
                    if filter == 'none':
                        df_data = load_data(choice, 'all', 'all')
                    # If filter is applied, we will fetch filtered data from the CSV
                    else:
                        if filter == 'month' or filter == 'both':
                            month = input('Which month would you like to filter the data on? (Jan/Feb/Mar/Apr/May/Jun) : ').lower()
                            if month not in month_dict:
                                valid_choice = False

                        if valid_choice and (filter == 'day' or filter == 'both'):
                            day = input('Which day would you like to filter the data on? (Sun/Mon/Tue/Wed/Thu/Fri/Sat) : ').lower()
                            if day not in day_dict:
                                valid_choice = False

                        if valid_choice:
                            df_data = load_data(choice, month, day)

                # Valid_choice will be TRUE only if all the inputs have been given correctly
                if valid_choice:

                    print('\nBased on your inputs, here are some interesting statistics...')

                    # First part of the information - popular times of travel
                    print_heading("POPULAR TIMES OF TRAVEL")
                    popular_month, count_popular_month = get_popular_data(df_data, 'Month')
                    print('The most common month of usage is : {} with a COUNT of {} entries'.format(
                        month_names.get(popular_month), count_popular_month))
                    popular_day, count_popular_day = get_popular_data(df_data, 'Day')
                    print(
                        'The most common day of usage is : {} with a COUNT of {} entries'.format(day_names.get(popular_day),
                                                                                                 count_popular_day))
                    print('The most common hour of usage is : {}:00 hrs with a COUNT of {} entries'.format(
                        *get_popular_data(df_data, 'Hour')))

                    # Second part of the information - popular stations and trips
                    print_heading("POPULAR STATIONS AND TRIPS")
                    print('The most common start station is : {} with a COUNT of {} entries'.format(
                        *get_popular_data(df_data, 'Start Station')))
                    print('The most common end station is : {} with a COUNT of {} entries'.format(
                        *get_popular_data(df_data, 'End Station')))
                    print('The most common trip is : {} with a COUNT of {} entries'.format(
                        *get_popular_data(df_data, 'Trip')))

                    # Third part of the information - trip duration
                    print_heading("TRIP DURATION")
                    print('The total travel time is : {} minutes for a COUNT of {} entries'.format(
                        df_data['Trip Duration'].sum(), df_data['Trip Duration'].count()))
                    print('The average travel time is : {} minutes'.format(df_data['Trip Duration'].mean()))

                    # Fourth part of the information - user information
                    print_heading("USER INFORMATION")
                    for label, count in df_data['User Type'].value_counts().items():
                        print('Number of {}s : {}'.format(label, count))

                    # Washington DC data does not contain 'Gender' and 'Year of birth' information
                    if choice in ('new york', 'chicago'):

                        # Print the Gender information from value_counts()
                        for label, count in df_data['Gender'].value_counts().items():
                            print('The number of {} users is : {}'.format(label, count))

                        # Print Year of Birth based information
                        print('\nThe earliest year of birth among the users is : {}'.format(int(df_data['Birth Year'].min())))
                        print('The most recent year of birth among the users is : {}'.
                            format(int(df_data['Birth Year'].max())))
                        print('The most common year of birth among the users is : {}'.
                            format(int(df_data['Birth Year'].mode()[0])))

                    # After statistical information, the user is asked if they want to see raw data
                    raw_data_choice = input('\nDo you want to see 5 rows of raw data ? (y/n) : ').lower() in accepted_yeses
                    raw_data = load_data(choice, 'all', 'all')

                    # Specify the columns that we need to show to the user to avoid showing newly created columns
                    list_of_cols = ['Start Time', 'End Time', 'Trip Duration', 'Start Station', 'End Station', 'User Type']

                    # Gender and Birth Year are available only for New York and Chicago
                    if choice in ('new york', 'chicago'):
                        list_of_cols.extend(['Gender', 'Birth Year'])
                    print('\n')

                    # If user wants to see raw data, start showing batches of data
                    if raw_data_choice:
                        for batch in get_data_in_batches(raw_data, raw_data_batch_size, list_of_cols):
                            for row in batch.iterrows():
                                for label, item in row[1].items():
                                    print('{} : {}'.format(label.upper(), item))
                                print("--------------------------------------------------------------------------------------------------------------")

                            # If the user wants to continue seeing more rows of data
                            raw_data_choice = input('\nPress \'y\' to continue seeing the next 5 rows of data...').lower() in accepted_yeses

                            if not raw_data_choice:
                                break

                # When any of the inputs given by user were invalid
                else:
                    print('\nInvalid input!')

        except Exception as e:

            # Handle exceptions if any occur to avoid showing the actual error message and logs to the user
            print('An unexpected error has occurred during execution.')

        finally:

            # The user can continue again if they want
            print("\n************************************************************************************************************************************************************")
            continue_flag = input('\nDo you want to continue? (y/n) : ').lower() in accepted_yeses

# This is to handle any issues of Keyboard interruptions during the execution.
# Since, there are a lot of user inputs in which user needs to type...

except KeyboardInterrupt:
    print('Invalid input!')

# The final message when user wants to quit the program
finally:
    print('Thank you for using the US BikeShare Data Interactive Tool!')

# Code ends here
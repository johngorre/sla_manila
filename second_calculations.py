import calendar
import requests
import pandas as pd
import datetime
from conn import config
import mysql.connector

#-----------------------------------------------------------------------------------------
# insert downtime incidents for both globe and rise
def insert_downtime_incident(start, end):

    # Convert the timestamp to a datetime object
    start_m = datetime.datetime.fromtimestamp(start['unix_timestamp'])
    end_m = datetime.datetime.fromtimestamp(end['unix_timestamp'])
    #get isp

    if start["device"] == 'ix2' and end["device"] == 'ix2':
        isp = 'pldt'
    if start["device"] == 'igc3' and end["device"] == 'igc3':
        isp = 'globe'
    if start["device"] == 'ix3' and end["device"] == 'ix3':
        isp = 'rise'
    # Get the month
    start_month = start_m.month
    end_month = end_m.month
    end_time = end['timestamp'][11:]
    downtime_duration_min = (end['unix_timestamp'] - start['unix_timestamp']) / 60
    start_date = start['timestamp']
    end_date = end['timestamp']
    location = "MANILA"

    if downtime_duration_min > 0:

        try:
            # Connect to the database
            connection = mysql.connector.connect(**config)
            if connection.is_connected():
                
                cursor = connection.cursor()

                # Define the SELECT query to check for existing record
                check_query = """SELECT COUNT(*) FROM downtime_incidents 
                                WHERE isp = %s AND downtime_minutes = %s AND start_date = %s AND end_date = %s 
                                AND location = %s AND start_month = %s AND end_month = %s AND end_time = %s"""
                                
                data = (isp, downtime_duration_min, start_date, end_date, location, start_month, end_month, end_time)
                
                # Execute the SELECT query to check if the record already exists
                cursor.execute(check_query, data)
                result = cursor.fetchone()
                
                # If the count is zero, insert the new record
                if result[0] == 0:
                    insert_logs = """INSERT INTO downtime_incidents (isp, downtime_minutes, start_date, end_date, location, start_month, end_month, end_time) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    cursor.execute(insert_logs, data)
                    connection.commit()

            
        except mysql.connector.Error as err:
            print(f"Error: {err}")

        finally:
            # Ensure the cursor and connection are closed after the operation
            if connection.is_connected():
                cursor.close()
                connection.close()

#-----------------------------------------------------------------------------------------
# clear zero valued datas in downtime.. used in push SLA
def delete_zero_downtime():
    try:
        # Connect to the database using the provided configuration
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Define the DELETE query
            delete_query = """DELETE FROM downtime_incidents WHERE downtime_minutes = 0;"""

            # Execute the DELETE query
            cursor.execute(delete_query)

            # Commit the changes to the database
            connection.commit()
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Ensure the cursor and connection are closed after the operation
        if connection.is_connected():
            cursor.close()
            connection.close()


#-----------------------------------------------------------------------------------------
# retrieve all the downtime incidents
def pull_downtime_incidents():

    downtime_incidents = []

    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Define the SELECT query to retrieve data
            select_query = """SELECT * FROM downtime_incidents"""

            # Execute the query
            cursor.execute(select_query)

            # Fetch all results
            rows = cursor.fetchall()

            # Print out the retrieved data
            for row in rows:
                downtime_incidents.append(row)  # Each 'row' is a tuple representing one record in the table
            
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()


    return downtime_incidents

#-----------------------------------------------------------------------------------------
# to check if the month record already exists and add if it doesn't exists
def check_month_year_exists(month, year):

    total_minutes = total_minutes_in_month(month, year)

    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            
            cursor = connection.cursor()

            # Define the SELECT query to check if the Month and Year already exist
            check_query = """SELECT COUNT(*) FROM monthly_downtime_statistics
                             WHERE Month = %s AND Year = %s AND TotalMinutesOfMonth = %s"""
            data = (month, year, total_minutes)

            # Execute the SELECT query to check if the month and year combination already exists
            cursor.execute(check_query, data)
            result = cursor.fetchone()

            # If the count is zero, insert the new month-year record
            if result[0] == 0:
                insert_query = """INSERT INTO monthly_downtime_statistics (Month, Year, TotalMinutesOfMonth)
                                  VALUES (%s, %s, %s)"""
                cursor.execute(insert_query, data)
                connection.commit()
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Ensure the cursor and connection are closed after the operation
        if connection.is_connected():
            cursor.close()
            connection.close()

#-----------------------------------------------------------------------------------------
# Get the total minutes of the month
def total_minutes_in_month(month_name, year):
    # Convert month name to a number
    month_number = list(calendar.month_name).index(month_name.capitalize())
    
    # Get the number of days in the given month
    days_in_month = calendar.monthrange(year, month_number)[1]
    
    # Calculate total minutes
    total_minutes = days_in_month * 24 * 60
    return total_minutes


#-----------------------------------------------------------------------------------------
# convert integer month to descriptive string month name
def int_to_month(month_number):
    return calendar.month_name[month_number]

#-----------------------------------------------------------------------------------------
# convert month to integer
def month_to_int(month_string):
    month_dict = {
        'January': 1,
        'February': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'August': 8,
        'September': 9,
        'October': 10,
        'November': 11,
        'December': 12
    }
    
    # Convert the month name to an integer using the dictionary
    return month_dict.get(month_string.capitalize(), "Invalid month name")


#-----------------------------------------------------------------------------------------
# retrieve all the monthly stat
def pull_monthly_stat(Month, Year):
    monthly_stat = []
    
    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)

        if connection.is_connected():
            cursor = connection.cursor()

            # Define the SELECT query with WHERE clause for filtering by month and year
            select_query = """
                SELECT * FROM monthly_downtime_statistics
                WHERE Year = %s AND Month = %s
            """
            
            # Execute the query with the provided month and year
            cursor.execute(select_query, (Year, Month))

            # Fetch all results
            rows = cursor.fetchall()

            # Append results to the list
            for row in rows:
                monthly_stat.append(row)  # Each 'row' is a tuple representing one record in the table
            
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()

    return monthly_stat

#-----------------------------------------------------------------------------------------
# update monthly stats function
def update (rise, globe, pldt, month, year):
    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Define the UPDATE query to modify data
            update_query = """
                UPDATE monthly_downtime_statistics
                SET GlobeDowntime = %s, RiseDowntime = %s, PldtDowntime = %s
                WHERE Month = %s AND Year = %s
            """
            
            # Execute the query with the provided values
            cursor.execute(update_query, (globe, rise, pldt, month, year))
            
            # Commit the transaction to apply the changes
            connection.commit()
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()

    globe = 0
    rise = 0


#-----------------------------------------------------------------------------------------
# pull everything from monthly stats
def pull_all_monthly_stat():
    monthly_downtime_statistics = []

    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Define the SELECT query to retrieve all data without filtering by month/year
            select_query = """SELECT * FROM monthly_downtime_statistics"""

            # Execute the query
            cursor.execute(select_query)

            # Fetch all results
            rows = cursor.fetchall()

            # Append each row to the list
            for row in rows:
                monthly_downtime_statistics.append(row)
            
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()

    return monthly_downtime_statistics


#-----------------------------------------------------------------------------------------
# calculate the number of minutes from the given time to 12 MN
def minutes_since_midnight(time_str):
    # Split the input time into hours, minutes, and seconds
    hours, minutes, seconds = map(int, time_str.split(':'))
    
    # Calculate total minutes since midnight, including seconds as a fraction of a minute
    total_minutes = hours * 60 + minutes + seconds / 60
    return total_minutes

#-----------------------------------------------------------------------------------------
# calculate the number of minutes from the given time to 12 MN
def push_history(month, year, rise, pldt, globe):
    
    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            
            cursor = connection.cursor()

            # Define the SELECT query to check if the Month and Year already exist
            check_query = """SELECT COUNT(*) FROM uptime_history 
                             WHERE Month = %s AND Year = %s"""
            data = (month, year)
            data2 = (month, year, rise, pldt, globe)

            # Execute the SELECT query to check if the month and year combination already exists
            cursor.execute(check_query, data)
            result = cursor.fetchone()

            # If the count is zero, insert the new month-year record
            if result[0] == 0:
                insert_query = """INSERT INTO uptime_history (Month, Year, rise_uptime, pldt_uptime, globe_uptime)
                                  VALUES (%s, %s, %s, %s, %s)"""
                cursor.execute(insert_query, data2)
                connection.commit()
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Ensure the cursor and connection are closed after the operation
        if connection.is_connected():
            cursor.close()
            connection.close()
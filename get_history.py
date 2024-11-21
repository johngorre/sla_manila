import mysql.connector as connection
from second_calculations import *
from data import *
import pandas as pd


#-----------------------------------------------------------------------------------------
# pull everything from update history
def pull_uptime_history():
    uptime_history = []

    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Define the SELECT query to retrieve all data without filtering by month/year
            select_query = """SELECT * FROM uptime_history"""

            # Execute the query
            cursor.execute(select_query)

            # Fetch all results
            rows = cursor.fetchall()

            # Append each row to the list
            for row in rows:
                uptime_history.append(row)
            
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()

    return uptime_history


#-----------------------------------------------------------------------------------------
# get missing

def find_missing_numbers(arr, cap=12):
    # Find the largest number in the array and subtract 1
    largest_number = max(arr) - 1
    
    # Cap the range at 'cap' (12)
    largest_number = min(largest_number, cap)
    
    # Create a full set of numbers from 1 to 'largest_number'
    full_range = set(range(1, largest_number + 1))
    
    # Convert the input array to a set for fast lookup
    arr_set = set(arr)
    
    # Find the missing numbers by subtracting the input array set from the full range set
    missing_numbers = list(full_range - arr_set)
    
    # Return the missing numbers sorted
    return sorted(missing_numbers)



def pull_sorted_uptime():
    # Create a cursor to execute queries
    downtime_incidents = []  # List to store records from the table

    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)

        if connection.is_connected():
            cursor = connection.cursor()

            # Define the SELECT query to retrieve data
            select_query = "SELECT * FROM uptime_history"

            # Execute the query
            cursor.execute(select_query)

            # Fetch all results
            rows = cursor.fetchall()

            # List of months in correct order
            month_order = [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]

            # Create a dictionary to map month names to their index (1 for January, 2 for February, etc.)
            month_index = {month: index for index, month in enumerate(month_order, start=1)}

            # Sort rows by the month (assuming the month is in the first column)
            sorted_rows = sorted(rows, key=lambda record: month_index[record[0]])

            # Add sorted rows to the downtime_incidents list
            downtime_incidents = sorted_rows

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()

    return downtime_incidents



#-----------------------------------------------------------------------------------------
history = pull_uptime_history()
months = []
current_year = datetime.datetime.now().year

for entry in history:
    months.append(month_to_int(entry[0]))


for entry in find_missing_numbers(sorted(months)):
    push_history(int_to_month(entry), current_year, 100, 100, 100)

sorted_uptimes = pull_sorted_uptime()

df = pd.DataFrame(sorted_uptimes, columns=['Month', 'Year', 'Rise Uptime', 'Pldt Uptime', 'Globe Uptime'])
df.to_csv('sample.csv', index=False)


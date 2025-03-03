import mysql.connector as connect
import datetime

# Manage SQL queries and configs
class SqlManager:
    def __init__(self):
        pass

    config = {
        "user": "root",
        "password": "9L2fsnBcVrRQTKrZd9hi9xrjBPZ7Bv",  # Replace with your MySQL root password
        "host": "10.0.48.130",
        "port": 3306,
        "database": "sla",  # Replace with your database name
    }

    # insert logs to db function
    # -----------------------------------------------------------------------------
    def insertIncidents(self, data):
        # Prepare the list of tuples to insert
        temp = [(datum['network'], datum['minutes'], datum['start'], datum['end'], datum['location']) for datum in data]

        try:
            # Establish the database connection
            connection = connect.connect(**self.config)
            cursor = connection.cursor()

            # Insert query to insert new incidents
            insert_query = """INSERT INTO downtime_incidents (isp, downtime_minutes, start_date, end_date, location)
                            VALUES (%s, %s, %s, %s, %s)"""

            # Check for existing similar incidents
            for datum in temp:
                # Check if the incident already exists
                check_query = """SELECT 1 FROM downtime_incidents 
                                WHERE isp = %s AND downtime_minutes = %s AND start_date = %s AND end_date = %s AND location = %s"""
                cursor.execute(check_query, datum)  # Pass datum as a tuple, no need to unpack
                result = cursor.fetchone()  # Fetch the result to ensure no unread results

                # If the result is None, it means no matching record was found, so we insert it
                if result is None:
                    cursor.execute(insert_query, datum)

            # Commit the transaction after all inserts
            connection.commit()

        except connect.Error as err:
            print(f"Error: {err}")

        finally:
            # Ensure the connection is closed properly
            if connection.is_connected():
                cursor.close()
                connection.close()

        # Call the method to clean up zero downtime entries (if applicable)
        self.delete_zero_downtime()

    #-----------------------------------------------------------------------------------------
    # clear zero valued datas in downtime.. used in push SLA
    def delete_zero_downtime(self):
        try:
            # Connect to the database using the provided configuration
            connection = connect.connect(**self.config)
            
            if connection.is_connected():
                cursor = connection.cursor()

                # Define the DELETE query
                delete_query = """DELETE FROM downtime_incidents WHERE downtime_minutes < 1;"""

                # Execute the DELETE query
                cursor.execute(delete_query)

                # Commit the changes to the database
                connection.commit()
            
        except connect.Error as err:
            print(f"Error: {err}")

        finally:
            # Ensure the cursor and connection are closed after the operation
            if connection.is_connected():
                cursor.close()
                connection.close()


    # -----------------------------------------------------------------------------------------
    # retrieve all the downtime incidents
    def pull_downtime_incidents(self):

        downtime_incidents = []

        try:
            # Connect to the database
            connection = connect.connect(**self.config)
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
                    downtime_incidents.append(
                        row
                    )  # Each 'row' is a tuple representing one record in the table
        except connect.Error as err:
            print(f"Error: {err}")

        finally:
            # Close cursor and connection
            if connection.is_connected():
                cursor.close()
                connection.close()

        return downtime_incidents
    

    def daily_insert(self, dailyLogs):
        try:
            # Connect to the database
            connection = connect.connect(**self.config)
            if connection.is_connected():
                cursor = connection.cursor()

                # Initialize a list to store values for batch insertion
                batch_values = []

                # Loop through the dailyLogs and check if the records already exist in the database
                for log in dailyLogs:
                    rise = log['rise']
                    globe = log['globe']
                    pldt = log['pldt']
                    start_time = log['start_day']
                    end_time = log['end_day']
                    
                    # Define the SELECT query to check if the specific start_time and end_time already exist
                    check_query = """SELECT COUNT(*) FROM daily_log
                                    WHERE start_time = %s AND end_time = %s"""
                    checker = (start_time, end_time)
                    
                    # Execute the SELECT query to check if the record exists
                    cursor.execute(check_query, checker)
                    result = cursor.fetchone()
                    
                    # If the record doesn't exist, add it to the batch insert list
                    if result[0] == 0:
                        batch_values.append((rise, globe, pldt, start_time, end_time))

                # If there are any new records to insert, execute a single batch INSERT
                if batch_values:
                    insert_query = """INSERT INTO daily_log (rise_uptime, globe_uptime, pldt_uptime, start_time, end_time)
                                    VALUES (%s, %s, %s, %s, %s)"""
                    cursor.executemany(insert_query, batch_values)
                    connection.commit()

        except connect.Error as err:
            print(f"Error: {err}")

        finally:
            # Ensure the cursor and connection are closed after the operation
            if connection.is_connected():
                cursor.close()
                connection.close()

    
    def insert_monthly_data(self, data):
        try:
            # Establish the connection
            connection = connect.connect(**self.config)
            cursor = connection.cursor()

            # Loop over the data to check if it exists and insert or update accordingly
            for datum in data:
                month, year, globe_uptime, rise_uptime, pldt_uptime, globe_downtime, rise_downtime, pldt_downtime = datum

                # Check if the record already exists
                query = "SELECT COUNT(*) FROM monthly_statistics WHERE month = %s AND year = %s"
                cursor.execute(query, (month, year))
                result = cursor.fetchone()

                if result[0] > 0:  # Record exists, update it
                    update_query = """
                        UPDATE monthly_statistics
                        SET globe_uptime = %s, rise_uptime = %s, pldt_uptime = %s, globe_downtime = %s, rise_downtime = %s, pldt_downtime = %s
                        WHERE month = %s AND year = %s
                    """
                    cursor.execute(update_query, (globe_uptime, rise_uptime, pldt_uptime, globe_downtime, rise_downtime, pldt_downtime, month, year))
                    print(f"Record for {month}-{year} updated.")
                else:  # Record doesn't exist, insert a new one
                    insert_query = """
                        INSERT INTO monthly_statistics (month, year, globe_uptime, rise_uptime, pldt_uptime, globe_downtime, rise_downtime, pldt_downtime)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (month, year, globe_uptime, rise_uptime, pldt_uptime, globe_downtime, rise_downtime, pldt_downtime))
                    print(f"Record for {month}-{year} inserted.")

            # Commit the transaction
            connection.commit()

            # Output the number of rows affected
            print(f"{cursor.rowcount} records inserted/updated successfully.")

        except connect.Error as err:
            print(f"Error: {err}")

        finally:
            # Closing the connection
            if connection.is_connected():
                cursor.close()
                connection.close()

    
    def addCurrentMonth(self):
        #check the current month exsist

        current_date = datetime.datetime.now()

        month = current_date.month
        year = current_date.year


        try:
            connection = connect.connect(**self.config)
            cursor = connection.cursor()
            
            # Check if the month and year already exist
            cursor.execute("SELECT 1 FROM monthly_statistics WHERE year = %s AND month = %s", (year, month))
            result = cursor.fetchone()

            if result:
                print(f"Month {month} and Year {year} already exist. No insertion needed.")
            else:
                # If not found, insert new record
                cursor.execute("INSERT INTO monthly_statistics (year, month, globe_uptime, rise_uptime, pldt_uptime, globe_downtime, rise_downtime, pldt_downtime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (year, month, 100, 100, 100, 0, 0, 0))
                connection.commit()
                print(f"Inserted Month {month} and Year {year} into the table.")
            
        except connect.Error as e:
            print(f"Error: {e}")
        finally:
            # Closing the connection
            if connection.is_connected():
                cursor.close()
                connection.close()

import calendar
import datetime
from conn import *
from second_calculations import *
import mysql.connector as connection
import pandas as pd

def parse_data(data):
    """Parse Prometheus data to extract relevant information."""
    results = []
    for result in data['data']['result']:
        instance = result['metric'].get('instance', 'N/A')
        device = result['metric'].get('device', 'N/A')
        # Iterate through all values for this instance
        for value in result['values']:
            #timestamp convertion start
            timestamp_raw = value[0] # Get timestamp
            timestamp_utc_time = datetime.datetime.fromtimestamp(timestamp_raw, tz=datetime.timezone.utc)
            timestamp_gmt_plus_8 = timestamp_utc_time + datetime.timedelta(hours=8)
            timestamp = timestamp_gmt_plus_8.strftime('%Y-%m-%d %H:%M:%S')
            #timestamp convertion end
            totalPackets = value[1]  # Get total packets corresponding to the timestamp
            results.append({
                'instance': instance,
                'device': device,
                'timestamp': timestamp,
                'unix_timestamp': timestamp_raw,
                'totalPackets': totalPackets
            })

    return results


#-----------------------------------------------------------------------------------------
# filter the logs from prometheus to readable sql data
def downtime_lookup(data):
    
    rise_downtime = []
    globe_downtime = []
    pldt_downtime = []
    cnt = 0
    cnt1 = 0
    cnt2 = 0

    for entry in data:
        if float(entry['totalPackets']) < 1 and entry['device'] == 'ix2':
            if cnt == 0:
                cnt = cnt + entry['unix_timestamp']
                pldt_downtime.append(entry)
            else:
                if entry['unix_timestamp'] - cnt == 15:
                    pldt_downtime.append(entry)
                    cnt = entry['unix_timestamp']
                else:
                    insert_downtime_incident(pldt_downtime[0], pldt_downtime[-1])
                    pldt_downtime.clear()
                    cnt = 0


        if float(entry['totalPackets']) < 1 and entry["device"] == 'ix3':
            if cnt1 == 0:
                cnt1 = cnt1 + entry['unix_timestamp']
                rise_downtime.append(entry)
            else:
                if entry['unix_timestamp'] - cnt1 == 15:
                    rise_downtime.append(entry)
                    cnt1 = entry['unix_timestamp']
                else:
                    insert_downtime_incident(rise_downtime[0], rise_downtime[-1])
                    rise_downtime.clear()
                    cnt1 = 0

        if float(entry['totalPackets']) < 1 and entry["device"] == 'igc3':
            if cnt2 == 0:
                cnt2 = cnt2 + entry['unix_timestamp']
                globe_downtime.append(entry)
            else:
                if entry['unix_timestamp'] - cnt1 == 15:
                    globe_downtime.append(entry)
                    cnt2 = entry['unix_timestamp']
                else:
                    insert_downtime_incident(globe_downtime[0], globe_downtime[-1])
                    globe_downtime.clear()
                    cnt2 = 0


#-----------------------------------------------------------------------------------------
# refresh downtime stats
def clear_monthly_stat():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create the TRUNCATE TABLE query
            truncate_query = "TRUNCATE TABLE monthly_downtime_statistics;"
            
            # Execute the query
            cursor.execute(truncate_query)
            connection.commit()

            
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        # Close cursor and connection
        if connection.is_connected():
            cursor.close()
            connection.close()


#-----------------------------------------------------------------------------------------
# to filter and push the final datas in the mysql database to be displayed in grafana
def push_sla_record():
    # 6 start month... 7 end month
    incidents = pull_downtime_incidents()
    delete_zero_downtime()
    
    
    monthly_downtime_minutes = 0
    #print(minutes_since_midnight(str(incidents[0][8])))
    for entry in incidents:
        if entry[6] == entry[7]:
            year = entry[3][:4]
            check_month_year_exists(int_to_month(entry[6]), int(year)) #check if record already existed and if not record will be added including the total minutes of the month
            if entry[1] == 'globe':
                globe_downtime = entry[2]
            else:
                globe_downtime = 0
            if entry[1] == 'rise':
                rise_downtime = entry[2]
            else:
                rise_downtime = 0
            if entry[1] == 'pldt':
                pldt_downtime = entry[2]
            else:
                pldt_downtime = 0

            update_monthly_records(float(globe_downtime), float(rise_downtime), float(pldt_downtime), int_to_month(entry[6]), int(year))

        else:
            #update the start month subtract the minutes from midnight if the is only one day of interval or add the total minutes from the last day of the month and until the end date
            #insert a new month record and add the total minutes of excess

            #step 1: identify the start and end month, in this case the start month is entry[6]
            #step 2: get the date and month of the end date and subtract it by 1. in this case its entry[4][8:10]
            minutes_from_midnight = float(minutes_since_midnight(entry[8]))
            excess_day = int(entry[4][8:10]) - 1
            minutes_to_add_first = minutes_until_end_of_day(entry[3][11:19])
            excess_day_in_minutes = float(excess_day) * 24 * 60

            #print(entry[2])

            #check if the end day is greater the 1
            if excess_day < 1:
                
                if entry[1] == 'globe':
                    globe_downtime_first = minutes_to_add_first
                    globe_downtime_second = minutes_from_midnight
                else:
                    globe_downtime_first = 0
                    globe_downtime_second = 0
                if entry[1] == 'rise':
                    rise_downtime_first = minutes_to_add_first
                    rise_downtime_second = minutes_from_midnight
                else:
                    rise_downtime_first = 0
                    rise_downtime_second = 0
                if entry[1] == 'pldt':
                    pldt_downtime_first = minutes_to_add_first
                    pldt_downtime_second = minutes_from_midnight
                else:
                    pldt_downtime_first = 0
                    pldt_downtime_second = 0

                update_monthly_records(float(globe_downtime_first), float(rise_downtime_first), float(pldt_downtime_first), int_to_month(entry[6]), entry[9])
                update_monthly_records(float(globe_downtime_second), float(rise_downtime_second), float(pldt_downtime_second), int_to_month(entry[7]), entry[9])

            else:
                
                if entry[1] == 'globe':
                    globe_downtime_first = minutes_to_add_first
                    globe_downtime_second = minutes_from_midnight + excess_day_in_minutes
                else:
                    globe_downtime_first = 0
                    globe_downtime_second = 0
                if entry[1] == 'rise':
                    rise_downtime_first = minutes_to_add_first
                    rise_downtime_second = minutes_from_midnight + excess_day_in_minutes
                else:
                    rise_downtime_first = 0
                    rise_downtime_second = 0
                if entry[1] == 'pldt':
                    pldt_downtime_first = minutes_to_add_first
                    pldt_downtime_second = minutes_from_midnight + excess_day_in_minutes
                else:
                    pldt_downtime_first = 0
                    pldt_downtime_second = 0

                print(globe_downtime_first)
                print(globe_downtime_second)
                print(rise_downtime_first)
                print(rise_downtime_second)
                print(pldt_downtime_first)
                print(pldt_downtime_second)

                update_monthly_records(float(globe_downtime_first), float(rise_downtime_first), float(pldt_downtime_first), int_to_month(entry[6]), entry[9])
                update_monthly_records(float(globe_downtime_second), float(rise_downtime_second), float(pldt_downtime_second), int_to_month(entry[7]), entry[9])

#-----------------------------------------------------------------------------------------
# calculations for the final table
def update_monthly_records(globe_downtime_in_minutes, rise_downtime_in_minutes, pldt_downtime_in_minutes, month, year):
    
    check_month_year_exists(month, year)
    stat = pull_monthly_stat(month, year)
    
    if stat[0][3] is None:
        rise = rise_downtime_in_minutes
    else:
        rise = stat[0][3] + rise_downtime_in_minutes

    if stat[0][4] is None:
        pldt = pldt_downtime_in_minutes
    else:
        pldt = stat[0][4] + pldt_downtime_in_minutes

    if stat[0][5] is None:
        globe = globe_downtime_in_minutes
    else:
        globe = stat[0][5] + globe_downtime_in_minutes

    #sql to update data
    update(rise, globe, pldt, month, year)


#-----------------------------------------------------------------------------------------
# uptime calculator
def uptime_calculator():
    stats = pull_all_monthly_stat()

    for entry in stats:
        globe_uptime = 100 - ((entry[5] * 100) / entry[2])
        rise_uptime = 100 - ((entry[3] * 100) / entry[2])
        pldt_uptime = 100 - ((entry[4] * 100) / entry[2])

        try:
            # Connect to the database
            connection = mysql.connector.connect(**config)
            
            if connection.is_connected():
                cursor = connection.cursor()

                # Define the UPDATE query to modify data
                update_query = """
                    UPDATE monthly_downtime_statistics
                    SET GlobeUptime = %s, RiseUptime = %s, PldtUptime = %s
                    WHERE Month = %s AND Year = %s
                """
                
                # Execute the query with the provided values
                cursor.execute(update_query, (globe_uptime, rise_uptime, pldt_uptime, entry[0], entry[1]))
                
                # Commit the transaction to apply the changes
                connection.commit()
            
        except mysql.connector.Error as err:
            print(f"Error: {err}")

        finally:
            # Close cursor and connection
            if connection.is_connected():
                cursor.close()
                connection.close()

#-----------------------------------------------------------------------------------------
# insert the current month;
def current_month():
    
    now = datetime.datetime.now()

    month = int_to_month(now.month)
    year = now.year
    total_minutes = total_minutes_in_month(month, year)
    globe_downtime = 0
    rise_downtime = 0
    globe_uptime = 100
    rise_uptime = 100

    try:
        # Connect to the database
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            
            cursor = connection.cursor()

            # Define the SELECT query to check if the Month and Year already exist
            check_query = """SELECT COUNT(*) FROM monthly_downtime_statistics
                             WHERE Month = %s AND Year = %s"""
            data = (month, year)
            data2 = (month, year, total_minutes, globe_downtime, rise_downtime, globe_uptime, rise_uptime)

            # Execute the SELECT query to check if the month and year combination already exists
            cursor.execute(check_query, data)
            result = cursor.fetchone()

            # If the count is zero, insert the new month-year record
            if result[0] == 0:
                insert_query = """INSERT INTO monthly_downtime_statistics (Month, Year, TotalMinutesOfMonth, GlobeDowntime, RiseDowntime, GlobeUptime, RiseUptime)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert_query, data2)
                connection.commit()
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Ensure the cursor and connection are closed after the operation
        if connection.is_connected():
            cursor.close()
            connection.close()


#-----------------------------------------------------------------------------------------
# insert histroy

def history():
    
    temp = pull_all_monthly_stat()
    
    for entry in temp:
        push_history(entry[0], entry[1], entry[6], entry[7], entry[8])
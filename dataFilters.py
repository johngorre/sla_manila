import datetime
import calendar
from sqlController import SqlManager


class DataFilter:
    def __init__(self):
        self.sql = SqlManager()
    
    # -----------------------------------------------------------------------------------------
    # filter the logs from prometheus to readable sql data
    def downtime_lookup(self, data):

        rise_downtime = []
        globe_downtime = []
        pldt_downtime = []
        globe_counter = 0
        rise_counter = 0
        pldt_counter = 0
        raw = []

        for entry in data:
            if float(entry["totalPackets"]) < 1 and entry["network"] == "globe":
                if globe_counter == 0:
                    globe_counter = globe_counter + entry["unix_timestamp"]
                    globe_downtime.append(entry)
                else:
                    if entry["unix_timestamp"] - globe_counter == 15:
                        globe_downtime.append(entry)
                        globe_counter = entry["unix_timestamp"]
                    else:
                        raw.append(self.listDowntime(globe_downtime[0], globe_downtime[-1]))
                        globe_downtime.clear()
                        globe_counter = 0

            if float(entry["totalPackets"]) < 1 and entry["network"] == "rise":
                if rise_counter == 0:
                    rise_counter = rise_counter + entry["unix_timestamp"]
                    rise_downtime.append(entry)
                else:
                    if entry["unix_timestamp"] - rise_counter == 15:
                        rise_downtime.append(entry)
                        rise_counter = entry["unix_timestamp"]
                    else:
                        raw.append(self.listDowntime(rise_downtime[0], rise_downtime[-1]))
                        rise_downtime.clear()   
                        rise_counter = 0

            if float(entry["totalPackets"]) < 1 and entry["network"] == "pldt":
                if pldt_counter == 0:
                    pldt_counter = pldt_counter + entry["unix_timestamp"]
                    pldt_downtime.append(entry)
                else:
                    if entry["unix_timestamp"] - pldt_counter == 15:
                        pldt_downtime.append(entry)
                        pldt_counter = entry["unix_timestamp"]
                    else:
                        raw.append(self.listDowntime(pldt_downtime[0], pldt_downtime[-1]))
                        pldt_downtime.clear()   
                        pldt_counter = 0

        return raw

    def listDowntime(self, start, end):

        downtimeItem = {}
        if (end['unix_timestamp'] - start['unix_timestamp'] / 60) > 0.5:
            downtimeItem = {
                "network": start['network'],
                "minutes": (end['unix_timestamp'] - start['unix_timestamp']) / 60,
                "start": start['unix_timestamp'],
                "end": end['unix_timestamp'],
                "location": "CEBU",
            }

        return downtimeItem
    
    # --------------------------------------------------------------------------------------
    # for rendering the month from the given unix stamp
    def get_midnight_stamp(self, input_timestamp):
        # Convert the input Unix timestamp to a datetime object
        dt = datetime.datetime.utcfromtimestamp(input_timestamp)

        # Create a new datetime object with the same year, month, and day, but at midnight
        midnight_dt = datetime.datetime(dt.year, dt.month, dt.day, 0, 0, 0)

        # Convert the midnight datetime object to a Unix timestamp
        midnight_timestamp = int(midnight_dt.timestamp())

        return midnight_timestamp
    
    # -----------------------------------------------------------------------------------------
    # check for downtime


    def check_for_downtime(self, start, end):

        downtime = self.sql.pull_downtime_incidents()
        rise = 0
        globe = 0
        pldt = 0

        for entry in downtime:

            network = entry[0]
            minutesDown = entry[1]
            downtimeStarts = entry[2]
            downtimeEnds = entry[3]
            location = entry[4]

            if network == "rise":
                # if the start and end of the day is between the start and end of the downtime incidents.
                if downtimeStarts <= start <= downtimeEnds and downtimeStarts <= end <= downtimeEnds:
                    rise += (end - start) / 60

                # if the start of the day is before the start of the downtime incident but the end of the day is between the start and end of the dowmtime.
                if not downtimeStarts <= start <= downtimeEnds and downtimeStarts <= end <= downtimeEnds:
                    rise += (end - downtimeStarts) / 60

                # if the end of the day exceeded the end of the downtime but the start of the day is between the start and end of the downtime
                if downtimeStarts <= start <= downtimeEnds and not downtimeStarts <= end <= downtimeEnds:
                    rise += (downtimeEnds - start) / 60
                

            if network == "globe":
                 # if the start and end of the day is between the start and end of the downtime incidents.
                if downtimeStarts <= start <= downtimeEnds and downtimeStarts <= end <= downtimeEnds:
                    globe += (end - start) / 60

                # if the start and end of the day is between the start and end of the downtime incidents.
                if not downtimeStarts <= start <= downtimeEnds and downtimeStarts <= end <= downtimeEnds:
                    globe += (end - downtimeStarts) / 60

                # if the start and end of the day is between the start and end of the downtime incidents.
                if downtimeStarts <= start <= downtimeEnds and not downtimeStarts <= end <= downtimeEnds:
                    globe += (downtimeEnds - start) / 60

            if network == "pldt":
                 # if the start and end of the day is between the start and end of the downtime incidents.
                if downtimeStarts <= start <= downtimeEnds and downtimeStarts <= end <= downtimeEnds:
                    pldt += (end - start) / 60

                # if the start and end of the day is between the start and end of the downtime incidents.
                if not downtimeStarts <= start <= downtimeEnds and downtimeStarts <= end <= downtimeEnds:
                    pldt += (end - downtimeStarts) / 60

                # if the start and end of the day is between the start and end of the downtime incidents.
                if downtimeStarts <= start <= downtimeEnds and not downtimeStarts <= end <= downtimeEnds:
                    pldt += (downtimeEnds - start) / 60

        return rise, globe, pldt

    # -----------------------------------------------------------------------------------------
    # insert daily logs
    def daily_insert(self, first_data, last_data):

        start_day = self.get_midnight_stamp(first_data["unix_timestamp"])  # get first day
        last_day = self.get_midnight_stamp(last_data["unix_timestamp"])  # get last day
        dailyLogs = []

        while start_day <= last_day:
            # check if theres a downtime in this range
            end_day = start_day + 86399

            # insert the check for downtime function
            downtime = self.check_for_downtime(start_day, end_day)
            rise = downtime[0]
            globe = downtime[1]
            pldt = downtime[2]

            # insert the start_day variable
            # insert the end day variable
            dailyLogs.append({
                'globe': globe,
                'rise': rise,
                'pldt': pldt,
                'start_day': start_day,
                'end_day': end_day
            })
            start_day = start_day + 86400

        self.sql.daily_insert(dailyLogs)


    def get_month_and_year(self, unix_timestamp):
        # Convert Unix timestamp to datetime object
        dt = datetime.datetime.utcfromtimestamp(unix_timestamp)
        
        # Extract the month and year
        month = dt.month
        year = dt.year
        
        return month, year
    
    def list_months(self, start_month, start_year, end_month, end_year):
        months_list = []
        
        current_year = start_year
        current_month = start_month
        
        # Loop through each month from the start to the end
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            # Append the current month and year to the list
            months_list.append((current_month, current_year))
            
            # Increment the month
            current_month += 1
            
            # If current_month exceeds 12, reset to 1 and increment the year
            if current_month > 12:
                current_month = 1
                current_year += 1
    
        return months_list
    
    def get_month_stamps(self, month, year):
        # First day of the month at 12:00 AM (midnight)
        first_day = datetime.datetime(year, month, 1, 0, 0, 0)
        
        # Last day of the month at 11:59 PM
        last_day = datetime.datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
        
        # Convert both to Unix timestamps
        first_day_timestamp = int(first_day.timestamp())
        last_day_timestamp = int(last_day.timestamp())
        
        return first_day_timestamp, last_day_timestamp
    
    def checks_for_mStat(self, start, end):

        downtime = self.sql.pull_downtime_incidents()
        rise = 0
        globe = 0
        pldt = 0

        for entry in downtime:

            network = entry[0]
            minutesDown = entry[1]
            downtimeStarts = entry[2]
            downtimeEnds = entry[3]
            location = entry[4]

            if network == "rise":
                # if the start and end of the day is between the start and end of the downtime incidents.
                if start <= downtimeStarts <= end and start <= downtimeEnds <= end:
                    rise += minutesDown

                # if the start of the day is before the start of the downtime incident but the end of the day is between the start and end of the dowmtime.
                if not start <= downtimeStarts <= end and start <= downtimeEnds <= end:
                    rise += (end - downtimeStarts) / 60

                # if the end of the day exceeded the end of the downtime but the start of the day is between the start and end of the downtime
                if start <= downtimeStarts <= end and not start <= downtimeEnds <= end:
                    rise += (downtimeEnds - start) / 60

            if network == "globe":
                # if the start and end of the day is between the start and end of the downtime incidents.
                if start <= downtimeStarts <= end and start <= downtimeEnds <= end:
                    globe += minutesDown

                # if the start of the day is before the start of the downtime incident but the end of the day is between the start and end of the dowmtime.
                if not start <= downtimeStarts <= end and start <= downtimeEnds <= end:
                    globe += (end - downtimeStarts) / 60

                # if the end of the day exceeded the end of the downtime but the start of the day is between the start and end of the downtime
                if start <= downtimeStarts <= end and not start <= downtimeEnds <= end:
                    globe += (downtimeEnds - start) / 60

            if network == "pldt":
                # if the start and end of the day is between the start and end of the downtime incidents.
                if start <= downtimeStarts <= end and start <= downtimeEnds <= end:
                    pldt += minutesDown

                # if the start of the day is before the start of the downtime incident but the end of the day is between the start and end of the dowmtime.
                if not start <= downtimeStarts <= end and start <= downtimeEnds <= end:
                    pldt += (end - downtimeStarts) / 60

                # if the end of the day exceeded the end of the downtime but the start of the day is between the start and end of the downtime
                if start <= downtimeStarts <= end and not start <= downtimeEnds <= end:
                    pldt += (downtimeEnds - start) / 60

        return rise, globe, pldt
    
    #-----------------------------------------------------------------------------------------
    # Get the total minutes of the month
    def get_month_minutes(self, month_name, year):
        # If month_name is an integer, use it directly, otherwise convert it to a number
        if isinstance(month_name, int):  # If it's already an integer, skip the conversion
            month_number = month_name
        else:  # Otherwise, convert the month name to a number
            month_number = list(calendar.month_name).index(month_name.capitalize())
        
        # Ensure the month_number is valid (1 <= month_number <= 12)
        if month_number < 1 or month_number > 12:
            raise ValueError("Invalid month name or number.")
        
        # Get the number of days in the given month
        days_in_month = calendar.monthrange(year, month_number)[1]
        
        # Calculate total minutes (days_in_month * 24 hours * 60 minutes)
        total_minutes = days_in_month * 24 * 60
        return total_minutes


    def push_monthly_stat(self):

        pushMonthlyStat = []

        downtime = self.sql.pull_downtime_incidents()
        if(len(downtime) < 1):
            print("to be edited")
        else:
            convertedStartMonth = self.get_month_and_year(downtime[0][2])
            convertedStartYear = self.get_month_and_year(downtime[0][3])
            convertedEndMonth = self.get_month_and_year(downtime[-1][2])
            convertedEndYear = self.get_month_and_year(downtime[-1][3])
            startMonth = convertedStartMonth[0]
            startYear = convertedStartYear[1]
            endMonth = convertedEndMonth[0]
            endYear = convertedEndYear[1]

            
            months = self.list_months(startMonth, startYear, endMonth, endYear)

            for entry in months:
                month = entry[0]
                year = entry[1]
                minutesOfMonth = self.get_month_minutes(month, year)

                stamps = self.get_month_stamps(month, year)
                monthlyDowntimes = self.checks_for_mStat(stamps[0], stamps[-1])
                pldtDowntime = monthlyDowntimes[2]
                globeDowntime = monthlyDowntimes[1]
                riseDowntime = monthlyDowntimes[0]

                globeUptime = 100 - ((globeDowntime / minutesOfMonth) * 100)
                riseUptime = 100 - ((riseDowntime / minutesOfMonth) * 100)
                pldtUptime = 100 - ((pldtDowntime / minutesOfMonth) * 100)

                pushMonthlyStat.append((month, year, globeUptime, riseUptime, pldtUptime, globeDowntime, riseDowntime, pldtDowntime))

            self.sql.insert_monthly_data(pushMonthlyStat)

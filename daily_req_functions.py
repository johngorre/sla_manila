import calendar
import datetime
import time


# function to convert time string to unix stamp
def date_to_unix(date_string):
    # Manually parse the date string into a time structure
    struct_time = time.strptime(date_string, "%Y-%m-%d %H:%M:%S")

    # Convert the time structure to a Unix timestamp
    unix_evolve_timestamp = int(time.mktime(struct_time))

    return unix_timestamp


def days_in_month(month, year):
    return calendar.monthrange(year, month)[1]

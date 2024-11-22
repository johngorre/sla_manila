import datetime

from daily_req_functions import *
from second_calculations import *

# declarations
first_month = days_in_month(7, 2024)
current_date = datetime.datetime.now()
month_now = current_date.month
day_now = current_date.day
downtime_incidents = pull_downtime_incidents()


print(first_month)

import  datetime
from data import *
from second_calculations import *
import pandas as pd


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
            minutes_from_midnight = minutes_since_midnight(entry[8])
            excess_day = int(entry[4][8:10]) - 1
            excess_day_in_minutes = float(excess_day) * 24 * 60

            #check if the end day is greater the 1
            if excess_day < 1:
                if entry[1] == 'globe':
                    globe_downtime = entry[2] - float(minutes_from_midnight)
                    globe_to_next_month = float(minutes_from_midnight)
                else:
                    globe_downtime = 0
                    globe_to_next_month = 0
                if entry[1] == 'rise':
                    rise_downtime = entry[2] - float(minutes_from_midnight)
                    rise_to_next_month = float(minutes_from_midnight)
                else:
                    rise_downtime = 0
                    rise_to_next_month = 0
                if entry[1] == 'pldt':
                    pldt_downtime = entry[2] - float(minutes_from_midnight)
                    pldt_to_next_month = float(minutes_from_midnight)
                else:
                    pldt_downtime = 0
                    pldt_to_next_month = 0

                update_monthly_records(float(globe_downtime), float(rise_downtime), float(pldt_downtime), int_to_month(entry[6]), int(year))
                update_monthly_records(float(7), float(rise_to_next_month), float(pldt_to_next_month), entry[7], int(entry[4][0:4]))

            else:

                if entry[1] == 'globe':
                    globe_downtime = entry[2] - float(minutes_from_midnight + excess_day_in_minutes)
                    globe_to_next_month = float(excess_day_in_minutes)
                else:
                    globe_downtime = 0
                    globe_to_next_month = 0
                if entry[1] == 'rise':
                    rise_downtime = entry[2] - float(minutes_from_midnight + excess_day_in_minutes)
                    rise_to_next_month = float(excess_day_in_minutes)
                else:
                    rise_downtime = 0
                    rise_to_next_month = 0
                if entry[1] == 'pldt':
                    pldt_downtime = entry[2] - float(minutes_from_midnight + excess_day_in_minutes)
                    pldt_to_next_month = float(excess_day_in_minutes)
                else:
                    pldt_downtime = 0
                    pldt_to_next_month = 0

                update_monthly_records(float(globe_downtime), float(rise_downtime), float(pldt_downtime), int_to_month(entry[6]), int(year))
                update_monthly_records(float(globe_to_next_month), float(rise_to_next_month), float(pldt_to_next_month), entry[7], int(entry[4][0:4]))

push_sla_record()
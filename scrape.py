import requests
import pandas as pd
from data import *
from conn import *


def main():
    # Configuration
    prometheus_url = "http://10.0.48.6:9090//"  # Replace with your Prometheus server URL
    query = 'irate(node_network_receive_packets_total{instance="10.0.48.1:9100", device=~"ix[23]|igc3"}[1m])[27w2d:15s]'  

    try:
         data = fetch_prometheus_data(prometheus_url, query)
         parsed_data = parse_data(data)
         downtime_lookup(parsed_data)
         clear_monthly_stat()
         push_sla_record()
         uptime_calculator()
         current_month()
         history()

    except Exception as e:
         print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
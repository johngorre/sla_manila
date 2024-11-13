import requests
import pandas as pd



#API to fetch data from prometheus (use URL for prometheus and your PromQL)
def fetch_prometheus_data(prometheus_url, query):
    """Fetch data from Prometheus API."""
    url = f"{prometheus_url}/api/v1/query"
    response = requests.get(url, params={'query': query})
    
    if response.status_code != 200:
        raise Exception(f"Error fetching data from Prometheus: {response.status_code} {response.text}")

    return response.json()


# Database connection details
config = {
    'user': 'root',
    'password': 'Rhevie',  # Replace with your MySQL root password
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'sla_tracker_manila'  # Replace with your database name
}
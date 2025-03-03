import requests
#----------------------------------------------------------------------------------------
# Manage Prometheus scrapes
class PromManager:
    def __init__(self):
        pass

    #----------------------------------------------------------------------------------------
    # Function to scrape Prometheus data
    def fetch_prometheus_data(self, prometheus_url, query):
        """Fetch data from Prometheus API."""
        url = f"{prometheus_url}/api/v1/query"
        response = requests.get(url, params={"query": query})

        if response.status_code != 200:
            raise Exception(
                f"Error fetching data from Prometheus: {response.status_code} {response.text}"
            )
        return response.json()

    #----------------------------------------------------------------------------------------
    # Initial data filter
    def arrangeData(self, data):
        """Parse Prometheus data to extract relevant information."""
        results = []
        for result in data["data"]["result"]:

            #check if what network the metric belongs
            if result["metric"].get("device", "N/A") == 'igc3':
                network = 'globe'
            if result["metric"].get("device", "N/A") == 'ix2':
                network = 'pldt'
            elif result["metric"].get("device", "N/A") == 'ix3':
                network = 'rise'

            # Iterate through all values for this instance
            for value in result["values"]:
                results.append(
                    {
                        "network": network,
                        "unix_timestamp": value[0],
                        "totalPackets": value[1],
                    }
                )

        return results
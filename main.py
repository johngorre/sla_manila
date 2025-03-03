# Manager class to tie everything together
from sqlController import SqlManager
from promController import PromManager
from dataFilters import DataFilter

class Manager:
    def __init__(self):
        self.prom = PromManager()
        self.filters = DataFilter()
        self.sql = SqlManager()

    def run(self):
        # Step one (Scrape Prometheus Data)
        prometheus_url = "http://10.0.48.130:9090"  # Replace with your Prometheus server URL
        query = 'irate(node_network_receive_packets_total{instance="10.0.48.1:9100", device=~"ix[23]"}[1m])[30w:15s]'
        
        promData = self.prom.fetch_prometheus_data(prometheus_url, query)
        arrangedData = self.prom.arrangeData(promData)
        downtimes = self.filters.downtime_lookup(arrangedData)
        self.sql.insertIncidents(downtimes)
        self.filters.daily_insert(arrangedData[0], arrangedData[-1])
        self.filters.push_monthly_stat()
        self.sql.addCurrentMonth()



if __name__ == "__main__":
    manager = Manager()
    manager.run()
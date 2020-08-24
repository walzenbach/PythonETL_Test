import pandas as pd
import requests
import json


class Extract:

    def __init__(self):
        # loading json file to use it across class methods
        self.data_sources = json.load(open('data_config.json'))
        self.api = self.data_sources['data_sources']['api']
        self.csv_path = self.data_sources['data_sources']['csv']

    def getAPI(self, api_name):
        # name attribute since we have multiple APIs in config
        # I can pass API link by passing name in the function argument

        api_url = self.api[api_name]
        response = requests.get(api_url)
        # this converts json data into python dict
        return response.json()

    def getCSV(self, csv_name):
        df = pd.read_csv(self.csv_path[csv_name])
        return df

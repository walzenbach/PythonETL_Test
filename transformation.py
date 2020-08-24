from DataSources import Extract
from DataLoad import MongoDB
import urllib
import pandas as pd
import numpy as np


class Transformation:

    def __init__(self, dataSource, dataSet):

        # creating Extract class object to fetch data
        extractObj = Extract()

        if dataSource == 'api':
            self.data = extractObj.getAPI(dataSet)
            funcName = dataSource+dataSet

            # getattr function takes in function name of class and calls it
            getattr(self, funcName)()
        elif dataSource == 'csv':
            self.data = extractObj.getCSV(dataSet)
            funcName = dataSource+dataSet
            getattr(self, funcName)()
        else:
            print('Unknown Data Source!!! Please Try again...')

    def api1(self):
        # transformations go here

        api1_data = self.data['results']

        # looping through data for sample transformation
        datalist = []
        for data in api1_data:
            for KPI in data['something']:
                dict = {}
                dict['column1'] = data['col1']
                dict['column2'] = data['col2']
                datalist.append(api1_data)

    # insert into DB

# CSV data transformation
    def csvCryptoExample(self):
        assetsCode = ['BTC', 'ETH', 'XRP', 'LTC']

        # coverting open, close, high and low price of crypto currencies into GBP values since current price is in Dollars
        # if currency belong to this list ['BTC','ETH','XRP','LTC']
        self.csv_df['open'] = self.csv_df[['open', 'asset']].apply(
            lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        self.csv_df['close'] = self.csv_df[['close', 'asset']].apply(
            lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        self.csv_df['high'] = self.csv_df[['high', 'asset']].apply(
            lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
        self.csv_df['low'] = self.csv_df[['low', 'asset']].apply(
            lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)

        # dropping rows with null values by asset column
        self.csv_df.dropna(inplace=True)

        # saving new csv file
        self.csv_df.to_csv('crypto-market-GBP.csv')

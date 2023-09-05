import tabula
import pandas as pd
from sqlalchemy import inspect

from database_utils import DatabaseConnector
import requests
import json
class DataExtractor:   
    """
    Defines methods that help extract data from different data sources such as
    CSV files, an API and an S3 bucket.
    """
    # Step 5 task3

    def read_rds_tables(self,engine,table_name):

        """This function reads a table from a database and returns a pandas dataframe
        
        Parameters
        ----------
        instance
            the database connector object
        table_name
            The name of the table you want to read from for user data
        
        Returns
        -------
            A dataframe
        """
        
        df = pd.read_sql_table(table_name,engine)
        
        # df.to_csv('user1_data.csv') 
        
        return df

    # step 2 task4
    def retrieve_pdf_data(self, link):
    
        card_df = tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf',pages='all')
        df = pd.concat(card_df)
        return df
    #step 1 and step2 task5
    def list_number_of_stores(self,header_dictionary):
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        header_dictionary = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
       
        response = requests.get(endpoint, headers=header_dictionary)
        return response.json()['number_stores']
    
        # step 3 task5
    def retrieve_stores_data(self, header_dictionary ):

        header_dictionary = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        store_number   = self.list_number_of_stores(header_dictionary)
        stores_list = []
        for i in range(store_number):
            endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(endpoint, headers=header_dictionary)
            stores_list.append( pd.json_normalize(response.json()))
        return pd.concat(stores_list)

if __name__== "__main__":
    instance = DatabaseConnector()
    data_extraction = DataExtractor()

    data_extraction.read_rds_tables(instance,'legacy_users')
    data_extraction.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #data_extraction.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
    data_extraction.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')




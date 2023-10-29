import tabula
import pandas as pd
from sqlalchemy import inspect

from database_utils import DatabaseConnector
import requests
import json
import boto3
class DataExtractor:   
    """
    Defines methods that help extract data from different data sources such as
    CSV files, an API and an S3 bucket.
    """
    # task3 step5

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

    # task4 step2
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
    
        # task5 step3
    def retrieve_stores_data(self, header_dictionary ):

        header_dictionary = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        store_number   = self.list_number_of_stores(header_dictionary)
        stores_list = []
        for i in range(store_number):
            endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            response = requests.get(endpoint, headers=header_dictionary)
            stores_list.append( pd.json_normalize(response.json()))
        return pd.concat(stores_list)
    #task6 step1
    def extract_from_s3(self,address):
        '''This function takes in a string that is the address of a csv file in an S3 bucket, and returns a
        pandas dataframe of the csv file
        
        Parameters
        ----------
        address
            the address of the file you want to extract
        
        Returns
        -------
            A dataframe
        '''
        client = boto3.client('s3')
        df = pd.read_csv(address)
        
        return df
    #task 8 
    def extract_from_s3_json(self, link):
        
        '''It takes a link as an input, makes a request to that link, and returns a dataframe of the json data.
        
        Parameters
        ----------
        link
            the link to the API
        
        Returns
        -------
            A dataframe
        '''
        
        response = requests.get(link)
        data = response.json()
        df = pd.DataFrame.from_dict(data)
        
        return df

if __name__== "__main__":
    instance = DatabaseConnector()
    data_extraction = DataExtractor()

    data_extraction.read_rds_tables(instance,'legacy_users')
    data_extraction.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    
    data_extraction.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')
    data_extraction.extract_from_s3('s3://data-handling-public/products.csv')
    data_extraction.extract_from_s3_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')




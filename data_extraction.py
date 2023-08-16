import tabula
import pandas as pd
from sqlalchemy import inspect
from tabula.io import read_pdf
from database_utils import DatabaseConnector
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

if __name__== "__main__":
    instance = DatabaseConnector()
    data_extraction = DataExtractor()

    data_extraction.read_rds_tables(instance,'legacy_users')
    data_extraction.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')




import numpy as np
import pandas as pd
import re
import datetime
import tabula

from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from sqlalchemy import create_engine


class DataCleaning:
       #Step 6 task3
    def clean_user_data(self,user_df):
        
        # The dataframe with the null values replaced with NaN .
        
        user_df=user_df.replace('NULL',np.nan)
         # Clean columns with first and last names
        user_df[['first_name', 'last_name']] = user_df[['first_name', 'last_name']].replace('[^a-zA-Z-]', np.nan, regex=True)
       
        #Drop row with duplicated
        user_df.drop_duplicates()
        # Removing rows with no user_uuid
        user_df = user_df[user_df['user_uuid'] != 'NULL']
        # drop the rows containing NaNs
        user_df= user_df[user_df.notna().any(axis=1)]
        #converts the date of birth column to datetime
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
        # check the incorrect dates for date of birth coloumn
        user_df = user_df[user_df['date_of_birth'].notna()]
        #converts the join date column to datetime
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], infer_datetime_format=True, errors = 'coerce')
        # check the  incorrect dates for join date
        user_df = user_df[user_df['join_date'].notna()]
        # check any null value
        user_df.isnull().sum()
        #Drop the null value
        user_df=user_df.dropna()
        # sort out some wrong country code entries for UK
        user_df.loc[(user_df['country'] == "United Kingdom") & (user_df['country_code'] != "GB"), 'country_code'] = 'GB'

        # remove some wrong entries
        user_df = user_df[user_df['country'].isin(['United Kingdom', 'Germany', 'United States'])]
       
        user_df = user_df.reset_index(drop=True)          
        return user_df
    
    # task 4 step 3
    def clean_card_data(self,card_df):
        #remove null value for each column
        card_df=card_df.dropna()
        # remove some wrong entries by removing the lines with wrong card provides
        card_df = card_df[card_df['card_provider'].isin(['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
        'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
        'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit'])]
        # Removes ? question marks from card numbers
        card_df['card_number'] = card_df['card_number'].apply(lambda x: re.sub(r'^\?+', '', x) if isinstance(x, str) else x)
        # removes non-numeric card numbers
        card_df = card_df[card_df['card_number'].apply(lambda x: str(x).isdigit())]
        #converts the card number column to an integer
        card_df['card_number'] = card_df['card_number'].astype('int64')
        # converts the card provider column to a category
        card_df['card_provider'] = card_df['card_provider'].astype('category') 
        # converts the payment date into a datetime object
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], infer_datetime_format=True, errors = 'coerce')
        # convert the card expiry date into a datetime object
        card_df['expiry_date'] =  pd.to_datetime(card_df['expiry_date'], infer_datetime_format=True, errors = 'coerce')
        card_df = card_df.reset_index(drop=True)
        return card_df
 

  
data_cleaning = DataCleaning()
data_extractor=DataExtractor()
db_connector= DatabaseConnector()
engine = db_connector.init_db_engine()
user_df=data_extractor.read_rds_tables(engine,'legacy_users')
print(user_df)
clean_user_df=data_cleaning.clean_user_data(user_df)
card_df=data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
print(card_df)
clean_card_df=data_cleaning.clean_card_data(card_df)
#clean_user_df.to_csv('clean_user_df13.csv')






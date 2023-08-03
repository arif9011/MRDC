import numpy as np
import pandas as pd
import datetime

from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from sqlalchemy import create_engine


class DataCleaning:
       #Step 6 task3
    def clean_user_data(self,user_df):
        #df = df.copy()
         
        # The dataframe with the null values replaced with NaN and then dropped.
        #df.replace(np.nan, 0)
        
        user_df=user_df.replace('NULL',np.nan)
        user_df= user_df.dropna()
       
         # Clean columns with first and last names
        user_df[['first_name', 'last_name']] = user_df[['first_name', 'last_name']].replace('[^a-zA-Z-]', np.nan, regex=True)
        #Drop row with duplicated
        user_df.drop_duplicates()
        # Removing rows with no user_uuid
        user_df = user_df[user_df['user_uuid'] != 'NULL']
        # drop the rows containing NaNs
        user_df= user_df[user_df.notna().any(axis=1)]
        # Clean columns with date of birth
        #converts the date of birth column to datetime
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
        # this statement allows to get rid of the rows with incorrect dates
        user_df = user_df[user_df['date_of_birth'].notna()]
        #converts the join date column to datetime
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], infer_datetime_format=True, errors = 'coerce')
        # this statement allows to get rid of the rows with incorrect dates
        user_df = user_df[user_df['join_date'].notna()]

        # check any null value
        user_df.isnull().sum()
        #Drop the null value for each colum
        user_df=user_df.dropna()
        # sort out some wrong country code entries for UK
        user_df.loc[(user_df['country'] == "United Kingdom") & (user_df['country_code'] != "GB"), 'country_code'] = 'GB'

        # remove some wrong entries
        user_df = user_df[user_df['country'].isin(['United Kingdom', 'Germany', 'United States'])]
        # replace 'GGB' with 'GB'
        # drop other wrong ones
        user_df['country_code'] = user_df['country_code'].replace('GGB','GB')
       
        user_df = user_df.reset_index(drop=True)          
        return user_df
        
data_cleaning = DataCleaning()
data_extractor=DataExtractor()
db_connector= DatabaseConnector()
engine = db_connector.init_db_engine()
user_df=data_extractor.read_rds_tables(engine,'legacy_users')
print(user_df)


clean_user_df=data_cleaning.clean_user_data(user_df)
clean_user_df.to_csv('clean_user_df.csv')



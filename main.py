from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


def main():
    # Creating an instance of the DatabaseConnector, DataExtractor and DataCleaner classes.
    db_connector = DatabaseConnector()
    data_extraction = DataExtractor()
    data_cleaning = DataCleaning()

    # Reading the legacy_users table from the database, cleaning the data and uploading it to the
    # dim_users table.
    user_df=data_extraction.read_rds_tables(db_connector,'legacy_users')
    clean_user_df = data_cleaning.clean_user_data(user_df)
    db_connector.upload_to_db(clean_user_df, 'dim_users')
    


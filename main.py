from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Creating an instance of the DatabaseConnector, DataExtractor and DataCleaner classes.
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()
    engine = db_connector.init_db_engine()
    
    # Reading the legacy_users table from the database, cleaning the data and uploading it to the 
    # dim_users table.
    user_df=data_extractor.read_rds_tables(engine,'legacy_users')
    print(user_df)
    clean_user_df=data_cleaning.clean_user_data(user_df)
    db_connector.upload_to_db(clean_user_df, 'dim_users')
    
    # This is reading the card_details.pdf file from the s3 bucket, cleaning the data and uploading it to
    # the dim_card_details table.
    card_df=data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(card_df)
    clean_card_df=data_cleaning.clean_card_data(card_df)
    db_connector.upload_to_db(clean_card_df, 'dim_card_details')
    
    # This is getting the data from the api, cleaning the data and uploading it to the dim_store_details table.
    
    store_df=data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')
    print(store_df)
    clean_store_df=data_cleaning.called_clean_store_data( store_df)
    db_connector.upload_to_db(clean_store_df, 'dim_store_details')
    
# This is reading the products.csv file from the s3 bucket, cleaning the data and uploading it to
# the dim_products table.
    product_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
    print(product_df)
    clean_product_df = data_cleaning.clean_products_data(product_df)
    db_connector.upload_to_db(clean_product_df, 'dim_products')

# Reading the orders_table from the database, cleaning the data and uploading it to the orders_table.
    order_df = data_extractor.read_rds_tables(engine, 'orders_table')
    print(order_df)
    clean_order_df = data_cleaning.clean_orders_data(order_df)
    db_connector.upload_to_db(clean_order_df, 'orders_table')
    
# This is reading the date_details.json file from the s3 bucket, cleaning the data and uploading it to
# the dim_date_times table.
    date_times_df=data_extractor.extract_from_s3_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    print(date_times_df)
    clean_date_times_df=data_cleaning.date_times_data(date_times_df)
    db_connector.upload_to_db(clean_date_times_df, 'dim_date_times')

main()
    


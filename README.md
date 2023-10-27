# MRDC
# Overview
#This project looks to implement good practise of data extraction, cleaning and querying to subsequently assist in making business decisions for an example real world environment.

#The goal will be to produce a system that extracts and stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. 

# Learning Objectives
#To implement data extraction from a range of endpoints utilising AWS RDS databases, PDF conversions, API extractions and S3 buckets containg CSV files.

#To clean the date from each source using pandas and postgresql.

# Project Structure
# Milestone 1: environment setup
It is necessary to create a postgreSQL database named sales_data with the following characteristics:

``` DATABASE_TYPE = 'postgresql'
HOST = 'localhost'
USER = 'postgres'
DATABASE = 'sales_data'
PORT = 5432
``` 
# Milestone 2: Data Extraction and cleaning data

#Extract and clean the data from each source

#Transfer the data over to a postgresql environment (pgAdmin 4)

# Milestone 3 : Create the database schema.

# Milestone 4 : Querying the data

 
 # Milestone 2 Extract and clean the data from the data sources
 
# Initialise the 3 project Classes
To extract, clean and upload the data we will be using 3 different Classes. data_extraction.py will contain methods that help extract from each data source (CSV files, an API and an S3 bucket) under a class DataExtractor. database_utils.py will connect and upload data to the pgAdmin database using Class DataExtractor. data_cleaning.py will contain methods to clean each data extraction before transfering the data using Class DataCleaning .

#  Extract and clean the user data
The user data is stored within an AWS RDS Database in the cloud. Credentials for the host, password, user, database and port are contained within the db_creds.yaml file which are used to access and extract from AWS.

The data comes from a variety of sources:

#user data: AWS database in the cloud.

#card details: stored in a PDF document in an AWS S3 bucket

#The stores data are stored as JSON files at several https urls and require the use of an API

#The product details are stored on a S3 server

#The orders are stored on a AWS RDS server

#Data has to be extracted, cleaned and stored in preparation for its storage on a PostgreSQL database.

Three classes provide the methods needed to:

#Connect to source and download data

#Clean the data

#Upload the data to a Postgresql database on localhost named sales_data

```
class DatabaseConnector:
    """
    Utility class. Defines the tools to connect, extract and upload data into the database.
    The methods contained will be fit to extract data from a particular data source.
    These sources include CSV files, an API and an S3 bucket.
    """

class DataCleaning:
    """
    Defines methods to clean data the user data from various datasources.
    """

class DataExtractor:
    """
    Defines methods that help extract data from different data sources such as
    CSV files, an API and an S3 bucket.
    """
```

The sequence of actions that lead to the creation of the company central database is coded in the main.py file called main.py

Finally, the cleaned data tables are updloaded to the dabaset through the upload_to_db method.

The tables in the schema are:

#user data: AWS database in the cloud --> dim_user_table

#card details: stored in a PDF document in an AWS S3 bucket --> dim_card_details

#The store data are stored as json files at several https urls and require the use of an API --> dim_store_data

#The product details are stored on a S3 server --> dim_products

#The orders are stored on a AWS RDS server --> orders_data

# Milestone 3 : Create the database schema.
Case the columns to the correct types
Although cleaning involved type casting the data correctly, often this can lead to errors if it is not taken across to pgadmin correctly. The data for these tables must be correctly typed so that we can create key constraints and call the data accurately. The types are viewable in pgadmin however it is important to note that you cannot change types to just anything. Only types that are related to the current type of the data column are provided. This means that changes must be made in using SQL queries first.


UUID's of all tables must be converted to the UUID data type, which is not a string to clarify. The example below casts the user_uuid within the orders table to the UUID data type.

``` SQL
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE uuid USING user_uuid ::uuid
```

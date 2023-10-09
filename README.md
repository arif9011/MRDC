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
# Milestone 2 - "Extract and clean the data from the data sources."

#Extract and clean the data from each source

#Transfer the data over to a postgresql environment (pgAdmin 4)

# Milestone 3 - "Create the database schema."

# Milestone 4 - "Querying the data"

 # Extract and clean the data from the data sources

 # Set up the database
To initialise the database, I use pgAdmin 4 and its easy functionality tying into VSCode for this project. The Database, sales_data, will serve as the blank canvas that extracted data will be imported to.
# Initialise the 3 project Classes
To extract, clean and upload the data we will be using 3 different Classes. data_extraction.py will contain methods that help extract from each data source (CSV files, an API and an S3 bucket) under a class DataExtractor. database_utils.py will connect and upload data to the pgAdmin database using Class DataExtractor. data_cleaning.py will contain methods to clean each data extraction before transfering the data using Class DataCleaning .

# Extract and clean the user data
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



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

-- Changing the columns datatype in oders_table

``` SQL
select * from orders_table;
ALTER TABLE IF EXISTS orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(50),
ALTER COLUMN store_code TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN product_quantity TYPE SMALLINT;

```
Card numbers, names, addresses must also be cast correctly. Varchars are used to limit the input taken from the user and provide a localised constraint on the column.
Quantities can be kept in any form of integer however I choose to represent these as ```SMALLINT``` given that quantities of stock generally don't get too large.
Dates must be typed correctly otherwise you cannot utilise time dependent queries as SQL will not understand the comparisons being made (date1 < date2). Fortunately during cleaning I chose to type cast columns to ```datetime64``` with pandas and this means SQL will provide the correct assumption that these are dates, subsequently providing the associated date types in pgadmin, such as ```date```

Along with type casting, additional columns were added to provide more insight into the data and help with categorisation. An example of this is providing a ```weight_class``` column in the products table that will divide the data into buckets for weights between 2:40:140:140+. 

### Create primary key relations

Now that the tables have the appropriate data types, the primary keys to each of the tables prefixed with dim are added. Each table will serve the orders_table which will be the single source of truth for the orders. The can be done in SQL using the code below which adds a primary key constraint in ```table``` on the data ```variable```.

``` sql
ALTER TABLE dim_card_details 
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times 
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products 
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details 
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users 
ADD PRIMARY KEY (user_uuid);


```

### Create foreign key constraints

The issue with linking foreign keys to primary keys is that if the data is not matched an error will be thrown. In connecting the orders table to each of the 5 primary keys, 3 worked as was the case for ```product_code``` for example which returned the constraint correctly implemented.
``` sql
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

DELETE FROM orders_table
WHERE product_code NOT IN (SELECT product_code FROM dim_products);


ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

DELETE FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);


ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

```
## Milestone 4 : Querying the data
## Task1: How Many stores does the store have and in which countries?

``` sql
SELECT 
       country_code, COUNT(country_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY
    total_no_stores DESC;
```

## Output :
![image](https://github.com/arif9011/MRDC/assets/115591569/071cd92d-ea3b-4341-8788-e99586373ea9)

## Task2: Which Locations currently have the most stores?
``` sql
SELECT 
      locality, COUNT(*) AS total_no_stores
FROM 
      dim_store_details
GROUP BY 
        locality
ORDER BY total_no_stores DESC
LIMIT 7;
```
## Output:

![image](https://github.com/arif9011/MRDC/assets/115591569/c2b41049-9c2e-4a03-ac17-4d493752b025)

## Task3: Which Months produce the average highest cost of sales typically?

``` sql
SELECT 
      SUM(orders_table.product_quantity * dim_products.product_price_£) as total_sales, dim_date_times.month 
FROM 
      dim_date_times 
JOIN 
      orders_table ON dim_date_times.date_uuid = orders_table.date_uuid 
JOIN 
      dim_products ON orders_table.product_code = dim_products.product_code 
GROUP BY 
      dim_date_times.month 
ORDER BY total_sales DESC;
```
## Output:

![image](https://github.com/arif9011/MRDC/assets/115591569/39982d87-bd7d-4213-b1fd-60ad6d6fdcf8)

## Task4: How many sales are coming from online? 
``` sql
SELECT COUNT(*) as number_of_sales, SUM(product_quantity) as product_quantity_count,
CASE 
    WHEN store_code LIKE 'WEB%' THEN 'Web'
    ELSE 'Offline'
END AS location    
FROM orders_table
GROUP BY location
```
## Output:

![image](https://github.com/arif9011/MRDC/assets/115591569/57013343-7c06-46eb-a31e-a64cc57ae44d)

## Task5: What Percentage of sales come through each type of store?

``` sql
SELECT
	store_type,
	SUM(product_quantity * product_price_£) AS total_sales,
	ROUND((SUM(product_quantity * product_price_£)::numeric / SUM(SUM(product_quantity * product_price_£)::numeric) OVER ()) * 100.0, 2) AS percentage_total
FROM
	orders_table
	JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY total_sales DESC;
```
## Output:
![image](https://github.com/arif9011/MRDC/assets/115591569/4ceeb707-1347-4187-95ad-b0c313335632)

## Task6: Which month in each year produced the highest cost of sales?
``` sql
select * from dim_date_times;
select 
      sum(product_quantity * product_price_£) as total_sales,
      year, month
from 
      orders_table
join 
      dim_date_times 
on 
      orders_table.date_uuid = dim_date_times.date_uuid
join 
     dim_products
on 
      orders_table.product_code = dim_products.product_code
group by year, month
order by 
      sum(product_quantity * product_price_£) desc
limit 20
```

## Output:
![image](https://github.com/arif9011/MRDC/assets/115591569/b1d21de5-5573-4ef9-b445-bf02a4480280)

## Task7: What is the staff headcount?
``` sql
select 
     sum(staff_numbers) as total_staff_numbers, country_code 
from 
     dim_store_details
group by country_code
order BY 
       sum(staff_numbers) desc
```

## Output:
![image](https://github.com/arif9011/MRDC/assets/115591569/4b4bd33d-d6ba-44b7-8086-4f68a5d7038a)

## Task8: Which German type store is selling the most?
``` sql
select 
       sum(product_price_£ * product_quantity) as total_sales,
       store_type, country_code 
from 
      orders_table
join 
      dim_products on
      orders_table.product_code = dim_products.product_code
join 
      dim_store_details on
      orders_table.store_code = dim_store_details.store_code
where 
      country_code = 'DE'
group by store_type, country_code
order BY sum(product_price_£ * product_quantity)
```
## Output:
![image](https://github.com/arif9011/MRDC/assets/115591569/7590eb1c-1eb0-4bab-bd27-1fd9b258121f)

## Task9: How quickly is the company making sales?
``` sql
WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD H:M:S') as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte
) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC;
```
## Output:

![image](https://github.com/arif9011/MRDC/assets/115591569/be627f81-8fb4-44bb-90c4-ee4f63ff4821)







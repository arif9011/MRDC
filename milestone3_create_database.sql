/*-- Task1 orders_table
--Change the data types to correspond to those seen in the table below.


--+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+
*/
select * from orders_table;

-- card number was cast as a bigint

-- Changing the columns datatype in oders_table
ALTER TABLE IF EXISTS orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(50),
ALTER COLUMN store_code TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN product_quantity TYPE SMALLINT;


-- Getting the maximum length of the varchar to change the type
SELECT MAX(CHAR_LENGTH(card_number)) FROM orders_table;
SELECT MAX(CHAR_LENGTH(store_code)) FROM orders_table;
SELECT MAX(CHAR_LENGTH(product_code)) FROM orders_table;

-- Changing the columns datatype according to max varchar
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11);

-- Task2  dim_users
/*The column required to be changed in the users table are as follows:

+----------------+--------------------+--------------------+
| dim_user_table | current data type  | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR(255)       |
| last_name      | TEXT               | VARCHAR(255)       |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(?)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |
+----------------+--------------------+--------------------+
 */

select * from dim_users;
-- Changing the columns datatype in dim_users
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE;

-- Task3 dim_store_details
/* 
There are two latitude columns in the store details table.
Using SQL, merge one of the columns into the other so you have one latitude column.
    This task in not performed as I sorted the latitude column in the data cleaning process
Then set the data types for each column as shown below:

+---------------------+-------------------+------------------------+
| store_details_table | current data type |   required data type   |
+---------------------+-------------------+------------------------+
| longitude           | TEXT              | FLOAT                  |
| locality            | TEXT              | VARCHAR(255)           |
| store_code          | TEXT              | VARCHAR(?)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
| latitude            | TEXT              | FLOAT                  |
| country_code        | TEXT              | VARCHAR(?)             |
| continent           | TEXT              | VARCHAR(255)           |
+---------------------+-------------------+------------------------+ */

select * from dim_store_details
UPDATE dim_store_details
SET longitude = NULL 
WHERE store_type = 'Web Portal';


-- Changing the columns datatype in dim_store_details
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::float,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(50),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);


-- Task4 dim_products
/*
You will need to do some work on the products table before casting the data types correctly.
The product_price column has a £ character which you need to remove using SQL. 
The team that handles the deliveries would like a new human-readable column added for the weight
so they can quickly make decisions on delivery weights.
Add a new column weight_class which will contain human-readable values based on the weight range of the product.

+--------------------------+-------------------+
| weight_class VARCHAR(?)  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+
 */

 
select * from dim_products;
-- Add weight_class column
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);
UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg BETWEEN 3 AND 40 THEN 'Mid_Sized'
    WHEN weight_kg BETWEEN 41 AND 140 THEN 'Heavy'
    ELSE 'Truck_required'
END;

-- Task5 
/*
After all the columns are created and cleaned, change the data types of the products table.

You will want to rename the removed column to still_available before changing its data type.

Make the changes to the columns to cast them to the following data types:

+-----------------+--------------------+--------------------+
|  dim_products   | current data type  | required data type |
+-----------------+--------------------+--------------------+
| product_price   | TEXT               | FLOAT              |
| weight          | TEXT               | FLOAT              |
| EAN             | TEXT               | VARCHAR(?)         |
| product_code    | TEXT               | VARCHAR(?)         |
| date_added      | TEXT               | DATE               |
| uuid            | TEXT               | UUID               |
| still_available | TEXT               | BOOL               |
| weight_class    | TEXT               | VARCHAR(?)         |
+-----------------+--------------------+--------------------+
*/
select * from dim_products;
-- Change Column name
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

-- Change columns datatypes
ALTER TABLE dim_products
ALTER COLUMN product_price_£ TYPE FLOAT,
ALTER COLUMN weight_kg TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL USING 
	CASE 
		WHEN still_available LIKE 'Still_available' THEN true
		ELSE false
	END;
	
-- Task6 dim date_times
/*Now update the date table with the correct types:

+-----------------+-------------------+--------------------+
| dim_date_times  | current data type | required data type |
+-----------------+-------------------+--------------------+
| month           | TEXT              | VARCHAR(?)         |
| year            | TEXT              | VARCHAR(?)         |
| day             | TEXT              | VARCHAR(?)         |
| time_period     | TEXT              | VARCHAR(?)         |
| date_uuid       | TEXT              | UUID               |
+-----------------+-------------------+--------------------+
*/
-- dim_date_times
select * from dim_date_times;
-- Change columns datatypes
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE CHAR(2),
ALTER COLUMN year TYPE CHAR(4),
ALTER COLUMN day TYPE CHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(11),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- Task7  dim_card_details
/*
Now we need to update the last table for the card details.

Make the associated changes after finding out what the lengths of each variable should be:

+------------------------+-------------------+--------------------+
|    dim_card_details    | current data type | required data type |
+------------------------+-------------------+--------------------+
| card_number            | TEXT              | VARCHAR(?)         |
| expiry_date            | TEXT              | VARCHAR(?)         |
| date_payment_confirmed | TEXT              | DATE               |
+------------------------+-------------------+--------------------+
*/
select * from dim_card_details;
-- Change columns datatypes
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;

-- Task8 making primary keys
/*Now that the tables have the appropriate data types we can begin adding the primary keys to each of the tables prefixed with dim.

Each table will serve the orders_table which will be the single source of truth for our orders.

Check the column header of the orders_table you will see all but one of the columns exist in one of our tables prefixed with dim.

We need to update the columns in the dim tables with a primary key that matches the same column in the orders_table.

Using SQL, update the respective columns as primary key columns.

*/
-- Making primary keys
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

SELECT product_code
FROM dim_products
WHERE product_code IS NULL;



-- Task 9 making foreign keys
/*
With the primary keys created in the tables prefixed with dim we can now create the foreign keys in the orders_table to reference the primary keys in the other tables.

Use SQL to create those foreign key constraints that reference the primary keys of the other table.

This makes the star-based database schema complete.
*/

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



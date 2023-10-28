-- milestone 4
-- task1 -- How many stores does the business have and in which countries?
/* 
The Operations team would like to know which countries we currently operate in and which country now has the most stores.
Perform a query on the database to get the information, it should return the following information:

+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             265 |
| DE       |             141 |
| US       |              34 |
+----------+-----------------+
 */
SELECT 
       country_code, COUNT(country_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY
    total_no_stores DESC;

-- Task2 -- Which locations currently have the most stores?
/*
The business stakeholders would like to know which locations currently have the most stores.
They would like to close some stores before opening more in other locations.
Find out which locations have the most stores currently. The query should return the following:

+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+
 */
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- Task3 Whitch months produce the average highest cost of sales typically?
/*
Query the database to find out which months typically have the most sales your query should return the following information:

+-------------+-------+
| total_sales | month |
+-------------+-------+
|   673295.68 |     8 |
|   668041.45 |     1 |
|   657335.84 |    10 |
|   650321.43 |     5 |
|   645741.70 |     7 |
|   645463.00 |     3 |
+-------------+-------+
 */
 
SELECT SUM(orders_table.product_quantity * dim_products.product_price_£) as total_sales, dim_date_times.month 
FROM dim_date_times 
JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid 
JOIN dim_products ON orders_table.product_code = dim_products.product_code 
GROUP BY dim_date_times.month 
ORDER BY total_sales DESC;

--task4 How many sales are coming from online?
/* The company is looking to increase its online sales.

They want to know how many sales are happening online vs offline.

Calculate how many products were sold and the amount of sales made for online and offline purchases.

You should get the following information:

+------------------+-------------------------+----------+
| numbers_of_sales | product_quantity_count  | location |
+------------------+-------------------------+----------+
|            26957 |                  107739 | Web      |
|            93166 |                  374047 | Offline  |
+------------------+-------------------------+----------+
*/

SELECT COUNT(*) as number_of_sales, SUM(product_quantity) as product_quantity_count,
CASE 
    WHEN store_code LIKE 'WEB%' THEN 'Web'
    ELSE 'Offline'
END AS location    
FROM orders_table
GROUP BY location

-- task5  What percentage of sales comes through from each type of store?
/*The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

Find out the total and percentage of sales coming from each of the different store types.

The query should return:

+-------------+-------------+---------------------+
| store_type  | total_sales | percentage_total(%) |
+-------------+-------------+---------------------+
| Local       |  3440896.52 |               44.87 |
| Web portal  |  1726547.05 |               22.44 |
| Super Store |  1224293.65 |               15.63 |
| Mall Kiosk  |   698791.61 |                8.96 |
| Outlet      |   631804.81 |                8.10 |
+-------------+-------------+---------------------+
*/

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

-- task6 Which month in each year produced the highest cost sales?

/* The company stakeholders want assurances that the company has been doing well recently.

Find which months in which years have had the most sales historically.

The query should return the following information:

+-------------+------+-------+
| total_sales | year | month |
+-------------+------+-------+
|    27936.77 | 1994 |     3 |
|    27356.14 | 2019 |     1 |
|    27091.67 | 2009 |     8 |
|    26679.98 | 1997 |    11 |
|    26310.97 | 2018 |    12 |
|    26277.72 | 2019 |     8 |
|    26236.67 | 2017 |     9 |
|    25798.12 | 2010 |     5 |
|    25648.29 | 1996 |     8 |
|    25614.54 | 2000 |     1 |
+-------------+------+-------+
*/
select * from dim_date_times;
select sum(product_quantity * product_price_£) as total_sales,
year, month
from orders_table
join dim_date_times 
on orders_table.date_uuid = dim_date_times.date_uuid
join dim_products
on orders_table.product_code = dim_products.product_code
group by year, month
order by sum(product_quantity * product_price_£) desc
limit 10

-- task7 What is our staff headcount?
/* The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

The query should return the values:

+---------------------+--------------+
| total_staff_numbers | country_code |
+---------------------+--------------+
|               13307 | GB           |
|                6123 | DE           |
|                1384 | US           |
+---------------------+--------------+
*/
select 
     sum(staff_numbers) as total_staff_numbers, country_code 
from 
     dim_store_details
group by country_code
order BY 
       sum(staff_numbers) desc

-- task8  Which German Store Type is selling the most?
/*
The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

The query will return:

+--------------+-------------+--------------+
| total_sales  | store_type  | country_code |
+--------------+-------------+--------------+
|   198373.57  | Outlet      | DE           |
|   247634.20  | Mall Kiosk  | DE           |
|   384625.03  | Super Store | DE           |
|  1109909.59  | Local       | DE           |
+--------------+-------------+--------------+

*/
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
-- task9 How quickly is the company making sales?
/*Sales would like the get an accurate metric for how quickly the company is making sales.

Determine the average time taken between each sale grouped by year, the query should return the following information:

 +------+-------------------------------------------------------+
 | year |                           actual_time_taken           |
 +------+-------------------------------------------------------+
 | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
 | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
 | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
 | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
 | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
 +------+-------------------------------------------------------+
 */

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
ORDER BY actual_time_taken DESC
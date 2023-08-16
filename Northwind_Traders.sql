--In this document I will be showing competency with Window Functions SQL Analytics using the  Northwind Traders Database


-- The first step is to understand the Database below I look at the schema to better understand what I am working with
--  Then I perform simple querys to learn more about customers , orders, and order details.

SELECT
    table_name as name,
    table_type as type
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW');



SELECT *
FROM customers
LIMIT 5;



SELECT *
FROM orders
LIMIT 5;



SELECT *
FROM order_details
LIMIT 5;

-- The goal of this query is to join the orders and employees tables to see who is responsible for each order:

SELECT 
    e.first_name || ' ' || e.last_name as full_name,
    o.order_id,
    o.order_date
FROM orders as o
JOIN employees as e ON o.employee_id = e.employee_id
LIMIT 10;

-- The goal of this query is to join the orders and customers tables to get more granularity about each customer:
SELECT 
    o.order_id,
    c.company_name,
    c.contact_name,
    o.order_date
FROM orders as o
JOIN customers as c ON o.customer_id = c.customer_id
LIMIT 10;


-- Here we are joining order_details, products, and orders to get  order information data including the product name and quantity:
SELECT 
    o.order_id,
    p.product_name,
    od.quantity,
    o.order_date
FROM order_details od
JOIN products as p ON od.product_id = p.product_id
JOIN orders as o ON od.order_id = o.order_id
LIMIT 10;

-- Here we use a window function on a CTE to Rank employees by sales performance

WITH EmployeeSales AS (
    SELECT Employees.Employee_ID, Employees.First_Name, Employees.Last_Name,
           SUM(Unit_Price * Quantity * (1 - Discount)) AS "Total Sales"
    FROM Orders 
    JOIN Order_Details ON Orders.Order_ID = Order_Details.Order_ID
    JOIN Employees ON Orders.Employee_ID = Employees.Employee_ID

    GROUP BY Employees.Employee_ID
)
SELECT Employee_ID, First_Name, Last_Name,
       RANK() OVER (ORDER BY "Total Sales" DESC) AS "Sales Rank"
FROM EmployeeSales;

-- From this we learn that Margeret Peacock is the top-seller and Steven Buchanan is the lowest-seller

-- Calculate running total of sales per month from a CTE

WITH MonthlySales AS (
    SELECT DATE_TRUNC('month', Order_Date)::DATE AS "Month", 
           SUM(Unit_Price * Quantity * (1 - Discount)) AS "Total Sales"
    FROM Orders 
    JOIN Order_Details ON Orders.Order_ID = Order_Details.Order_ID
    GROUP BY DATE_TRUNC('month', Order_Date)
)
SELECT "Month", 
       SUM("Total Sales") OVER (ORDER BY "Month") AS "Running Total"
FROM MonthlySales
ORDER BY "Month";


-- Calculate the month-over-month sales growth rate from multiple CTE
WITH MonthlySales AS (
    SELECT EXTRACT('month' from Order_Date) AS Month, 
           EXTRACT('year' from Order_Date) AS Year, 
           SUM(Unit_Price * Quantity * (1 - Discount)) AS TotalSales
    FROM Orders 
    JOIN Order_Details ON Orders.Order_ID = Order_Details.Order_ID
    GROUP BY EXTRACT('month' from Order_Date),  EXTRACT('year' from Order_Date)
),
LaggedSales AS (
    SELECT Month, Year, 
           TotalSales, 
           LAG(TotalSales) OVER (ORDER BY Year, Month) AS PreviousMonthSales
    FROM MonthlySales
)
SELECT Year, Month,
       ((TotalSales - PreviousMonthSales) / PreviousMonthSales) * 100 AS "Growth Rate"
FROM LaggedSales;

-- Identify customers with above-average order values using a CTE

WITH OrderValues AS (
    SELECT Orders.Customer_ID, 
           Orders.Order_ID, 
           SUM(Unit_Price * Quantity * (1 - Discount)) AS "Order Value"
    FROM Orders 
    JOIN Order_Details ON Orders.Order_ID = Order_Details.Order_ID
    GROUP BY Orders.Customer_ID, Orders.Order_ID
)
SELECT Customer_ID, 
       Order_ID, 
       "Order Value",
       CASE 
           WHEN "Order Value" > AVG("Order Value") OVER () THEN 'Above Average'
           ELSE 'Below Average'
       END AS "Value Category"
FROM OrderValues LIMIT 10;


-- Using a CTE and a window function we calculate the percentage of total sales for each product categoryu

WITH CategorySales AS (
    SELECT Categories.Category_ID, Categories.Category_Name,
           SUM(Products.Unit_Price * Quantity * (1 - Discount)) AS "Total Sales"
    FROM Categories
    JOIN Products ON Categories.Category_ID = Products.Category_ID
    JOIN Order_Details ON Products.Product_ID = Order_Details.Product_ID
    GROUP BY Categories.Category_ID
)
SELECT Category_ID, Category_Name,
       "Total Sales" / SUM("Total Sales") OVER () * 100 AS "Sales Percentage"
FROM CategorySales;

-- Combining a CTE and Window Functions we find the top 3 products sold in each category

WITH ProductSales AS (
    SELECT Products.Category_ID, 
           Products.Product_ID, Products.Product_Name,
           SUM(Products.Unit_Price * Quantity * (1 - Discount)) AS "Total Sales"
    FROM Products
    JOIN Order_Details ON Products.Product_ID = Order_Details.Product_ID
    GROUP BY Products.Category_ID, Products.Product_ID
)
SELECT Category_ID, 
       Product_ID, Product_Name,
       "Total Sales"
FROM (
    SELECT Category_ID, 
           Product_ID, Product_Name,
           "Total Sales", 
           ROW_NUMBER() OVER (PARTITION BY Category_ID ORDER BY "Total Sales" DESC) AS row_num
    FROM ProductSales
) tmp
WHERE row_num <= 3;






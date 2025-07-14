--CREATE VIEW CALENDAR---
-----------------------------------------

CREATE OR ALTER view gold.calendar AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) as Query1

--CREATE VIEW PRODUCTS---
-----------------------------------------

create or ALTER view gold.products AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/AdventureWork_Products/',
    FORMAT = 'PARQUET'
) as Query1

--CREATE VIEW customers---
-----------------------------------------

create or alter view gold.customers AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) as Query1

--CREATE VIEW Returns---
-----------------------------------------

create or alter view gold.returns AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) as Query1

--CREATE VIEW Sales---
-----------------------------------------

create or alter view gold.sales AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) as Query1


--CREATE VIEW Territories---
-----------------------------------------

create OR ALTER view gold.ter AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) as Query1


--CREATE VIEW Subcatagories---
-----------------------------------------

create OR ALTER view gold.subcat AS
select * from 
OPENROWSET(
    BULK 'https://asdatalake1.blob.core.windows.net/silver/Product_Subcategories/',
    FORMAT = 'PARQUET'
) as Query1

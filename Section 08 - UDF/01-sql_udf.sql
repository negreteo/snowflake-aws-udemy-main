use role sysadmin;
USE WAREHOUSE COMPUTE_WH;
use schema "ECOMMERCE_DB"."ECOMMERCE_DEV";

-- SIMPLE UDF ----

CREATE or replace FUNCTION sum_values(a number,b number)
  RETURNS number
  LANGUAGE SQL 
  AS
  $$
    SELECT a+b as res
  $$;


SELECT sum_values(2,3);


-- Scalar Function to calculate sales quantity by Supplier 

CREATE or replace FUNCTION sales_qty_by_supplier(ship_date date,supplier_key number)
  RETURNS NUMERIC(11,2)
  LANGUAGE SQL
  AS
  $$
    SELECT SUM(l_quantity) as total_quantity_shipped
        FROM "ECOMMERCE_DB"."ECOMMERCE_DEV"."LINEITEM"
        WHERE L_SHIPDATE =ship_date AND l_suppkey=supplier_key
  $$;

select L_SHIPDATE, l_suppkey
from lineitem
order by l_shipdate desc
limit 100;

-- Use the above Scalar function in a query 

SELECT sales_qty_by_supplier('1998-05-22', 8939312);

SELECT
    sales_qty_by_supplier('1998-05-22', 8939312) AS supplier_sales,
    s_suppkey 
FROM 
    "ECOMMERCE_DB"."ECOMMERCE_DEV"."SUPPLIER"
WHERE supplier_sales > 0;

--- TABULAR UDF ----

CREATE or REPLACE FUNCTION sales_qty_by_supplier(ship_date varchar,supplier_key number)
  RETURNS table(supplier_key number, qty_sold number)
  AS
  $$
    select 
        lt.L_SUPPKEY ,
        sum(L_QUANTITY) as qty_sold 
    from 
        "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."SUPPLIER" sp 
    join 
        "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."LINEITEM" lt on sp.S_SUPPKEY = lt.L_SUPPKEY
    where lt.l_shipdate = ship_date and lt.L_SUPPKEY = supplier_key
    group by 1
    having qty_sold > 0
  $$;

-- Call the above tabular UDTF 
select * from table (sales_qty_by_supplier('1996-06-27',23661));

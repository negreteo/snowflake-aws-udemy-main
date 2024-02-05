USE role task_owner;
USE warehoUSE prod_xl;
USE  ecommerce_db.ecommerce_liv;

 --- CREATE a table to store the results ----
CREATE OR REPLACE TABLE DAILY_AGGREGATED_SUMMARY (
	SUM_QTY NUMBER(24,2),
	TOTAL_BASE_PRICE NUMBER(24,2),
	TOTAL_DISCOUNT_PRICE NUMBER(37,4),
	TOTAL_CHARGE NUMBER(38,6),
	ORDER_COUNT NUMBER(18,0),
	SHIPPED_DATE DATE,
	SHIPPED_MODE VARCHAR(10)
);

CREATE OR REPLACE TABLE ORDERS_BY_SHIPMODE (
	TOTAL_ORDERS NUMBER(30,0),
	TOTAL_DISCOUNT NUMBER(38,0),
	SHIPPED_DATE DATE,
	SHIPPED_MODE VARCHAR(10)
);

-- Standalone Task, when the tasks is CREATEd the default state is SUSPENDED ----- 
CREATE TASK TSK_DAILY_SALES_SUMMARY
warehoUSE = prod_xl
schedule = 'using cron 0 8 * * * UTC' AS 
INSERT INTO "ECOMMERCE_DB"."ECOMMERCE_LIV"."DAILY_AGGREGATED_SUMMARY"
SELECT       
       sum(l_quantity) AS sum_qty,
       sum(l_extendedprice) AS total_base_price,
       sum(l_extendedprice * (1-l_discount)) AS total_discount_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) AS total_charge,
       count(*) AS order_count,
       date(l_shipdate) AS shipped_date,
       l_shipmode AS shipped_mode
 FROM
       "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
 WHERE
       shipped_date = '1996-07-17'
 GROUP BY
       shipped_date,
       shipped_mode;

-- Shows the list of tasks
SHOW tasks; 

-- Resumes the suspended task
ALTER TAKS TSK_DAILY_SALES_SUMMARY RESUME; 

-- Dependent Task ----- 

CREATE TASK TSK_ORDERS_BY_SHIPMODE
WAREHOUSE = prod_xl
AFTER TSK_DAILY_SALES_SUMMARY AS -- assigns the predecesor task
INSERT INTO "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS_BY_SHIPMODE" 
SELECT 
    round(sum(order_count)) AS total_orders , 
    round(sum(total_discount_price),0) AS total_discount,
    shipped_date,
    shipped_mode
FROM 
    daily_aggregated_summary
GROUP BY 3,4


---- Check Task History ------ 
-- You are not billed for the creation of tasks, only for the compute resources usage
USE role accountadmin;

-- Queries the executed tasks history in the last hour
SELECT *
  FROM table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'task_name'));




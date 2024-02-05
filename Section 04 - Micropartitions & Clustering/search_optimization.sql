-- USE ROLE SYSADMIN;
USE ROLE ACCOUNTADMIN;
USE SCHEMA "ECOMMERCE_DB"."ECOMMERCE_LIV";
USE WAREHOUSE COMPUTE_WH;

create or replace database test_db_so;

create or replace schema test_schema;

use schema test_db_so.test_schema;

-- Create a table with no Search optimization feature enabled ----
create or replace table lineitem_no_so clone "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM";
--  OR ---
create or replace table lineitem_no_so cluster by(l_shipdate) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM";


-- Create a table and enable the Search optimization feature ----
create or replace table lineitem_so clone "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM";

-- Get the estimate search optimization cost
select system$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('lineitem_no_so');

-- Enable the Search Optimization Feature
alter table lineitem_so add search optimization;

-- View the search_optimization = ON, and wait for search_optimization_progress to change from 0 to 100
show tables like '%lineitem_so%';

-- Point lookup query ---
select * from lineitem_no_so where l_orderkey='2412266214' limit 10;
select * from lineitem_so where l_orderkey='2412266214' limit 10;
select * from lineitem_no_so where l_orderkey='35786325' limit 10;

-- Disable the Search Optimization Feature in the table 
alter table lineitem_so drop search optimization;

-- Clear Result and WH Cache ---
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
alter warehouse prod_xl suspend;
alter warehouse prod_xl resume;
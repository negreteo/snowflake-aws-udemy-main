use schema "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000";
-- Create a large warehouse, 1 single cluster, named ETL_XL
use warehouse ETL_XL;

--- Check Clustering info in a table ---
select system$clustering_information('LINEITEM');

-- shows table information with name like 
show tables like '%LINE%';

-- This returns 'Clustering key columns contain high cardinality key PS_SUPPKEY which might result 
-- in expensive re-clustering. Consider reducing the cardinality of clustering keys'.
-- Cardinality is the number of distinct values of the column in question, when a clustering key contains 
-- high cardinality, it refers that the number of distinct values is high.
select system$clustering_information('PARTSUPP','ps_suppkey');

-- Gets the number of partitions in the table
-- Check Distinct values before selecting the clustering key.
select count(1),count(distinct ps_suppkey) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PARTSUPP;

-- Clear Cache (use when working with benchmarks)
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

---- Run SQL commands-----

select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" limit 20000;
select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" where l_shipdate='1998-12-01';
select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" where l_shipdate in ('1998-12-01','1998-09-20');
select L_ORDERKEY,L_PARTKEY,L_SUPPKEY from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" where l_shipdate in ('1998-12-01','1998-09-20');

-- Change Schema ----

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

select system$clustering_information('LINEITEM');

--- Check total number of partitions in the table ----

select * from ORDERS limit 2000;

-- Create a 3XL Warehouse ---
    ALTER WAREHOUSE "ETL_XL" 
    SET WAREHOUSE_SIZE = 'XXXLARGE' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1 
    SCALING_POLICY = 'STANDARD' COMMENT = '';

--- Re-create the table with a clustering key ---- 
create or replace table LINEITEM cluster by (L_SHIPDATE)
as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM";

-- Alter Warehouse after the above execution----
    ALTER WAREHOUSE "ETL_XL" 
    SET WAREHOUSE_SIZE = 'MEDIUM' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1 
    SCALING_POLICY = 'STANDARD' 
    COMMENT = '';

--  Get credit usage from automatic reclustering ---
select * 
from table(
        snowflake.information_schema.automatic_clustering_history
        (
            date_range_start=>dateadd(h, -12, current_timestamp)
        )
        );

-- Disable automatic re-clustering ---
alter table LINEITEM suspend recluster;




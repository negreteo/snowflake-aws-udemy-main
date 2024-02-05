use role accountadmin;

--use role sysadmin; 

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";
use warehouse compute_wh;

--alter table lineitem cluster by (L_SUPPKEY,L_SHIPDATE);

create table lineitem_clone clone lineitem;

alter table lineitem_clone cluster by (L_SUPPKEY,L_SHIPDATE);

-- Returns clustering information, including average clustering depth, 
--for a table based on one or more columns in the table.
select system$clustering_information('lineitem_clone','(L_SUPPKEY,L_SHIPDATE)')

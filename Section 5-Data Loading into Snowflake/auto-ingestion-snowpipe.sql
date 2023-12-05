use role sysadmin;
-- use role accountadmin
use schema ecommerce_db.ecommerce_dev;

-- Create a stage for lineitem table  ---
create or replace stage stg_lineitem_csv_dev
storage_integration = aws_sf_data
url = 's3://s3-on-dev-ecn1-snowflake-001/ecommerce_dev/lineitem/lineitem_snowpipe/csv/'
file_format = csv_load_format;

list @stg_lineitem_csv_dev;

create or replace pipe lineitem_pipe auto_ingest=true as
copy into lineitem from @stg_lineitem_csv_dev ON_ERROR = continue;

show pipes;

-- truncate lineitem table if not empty
select count(1) as lineitem_total from lineitem; --767
truncate table lineitem;

select * from information_schema.load_history where table_name='LINEITEM' order by last_load_time desc limit 10;

use role accountadmin;

-- Switch role to accountadmin before running this command -- 
select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-3,current_timestamp()),
    pipe_name=>'lineitem_pipe'));

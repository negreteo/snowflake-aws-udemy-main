USE ROLE accountadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ecommerce_db;

-- Create an storage integration to store a generated identity and access management (IAM) entity for the external cloud storage.
CREATE or REPLACE STORAGE INTEGRATION aws_sf_data
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::965642570530:role/iamr-on-dev-gbl-snowflake-aws-001'
  STORAGE_ALLOWED_LOCATIONS = ('s3://s3-on-dev-ecn1-snowflake-001');

-- Grant permission to the storage integration to the sysadmin role. 
GRANT USAGE ON INTEGRATION aws_sf_data TO ROLE sysadmin;

-- Grant execute permissions to the sysadmin role.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;

-- Grant access to create stages for loading data in the schema.
GRANT CREATE STAGE ON SCHEMA "ECOMMERCE_DB"."ECOMMERCE_DEV" TO ROLE sysadmin;

-- The pipeline will be created using the sysadmin role.
USE ROLE sysadmin;

USE SCHEMA "ECOMMERCE_DB"."ECOMMERCE_DEV";

-- Get the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Update the AWS IAM Role Policy (Trust Relationships)
-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::730335278937:user/2kti0000-s
-- STORAGE_AWS_EXTERNAL_ID: LXB17742_SFCRole=2_pqsSDVOnZuHLSVxsHfSOjKi+u/s=
DESC INTEGRATION aws_sf_data;

-- Create a file format to read a JSON file.
CREATE OR REPLACE FILE FORMAT json_load_format TYPE = 'JSON';

-- Create a stage for loading data from files into Snowflake tables and unloading data from tables into files.
CREATE OR REPLACE STAGE stg_lineitem_json_dev
STORAGE_INTEGRATION = aws_sf_data
URL = 's3://s3-on-dev-ecn1-snowflake-001/streams_dev/'
FILE_FORMAT = json_load_format;

-- Show the created stage information.
LIST @stg_lineitem_json_dev;

-- Create a table where data will initially be stored.
CREATE OR REPLACE TABLE lineitem_raw_json (src VARIANT);

-- Create delta stream to track inserts and updates.
CREATE OR REPLACE STREAM lineitem_std_stream ON TABLE lineitem_raw_json;

-- Create a recurring task that checks when the stream has data for inserts and updates.
CREATE OR REPLACE TASK lineitem_load_tsk 
WAREHOUSE = compute_wh
SCHEDULE = '1 minute'
WHEN system$stream_has_data('lineitem_std_stream')
AS 
MERGE INTO lineitem AS li 
USING 
(
   SELECT 
        SRC:L_ORDERKEY AS L_ORDERKEY,
        SRC:L_PARTKEY AS L_PARTKEY,
        SRC:L_SUPPKEY AS L_SUPPKEY,
        SRC:L_LINENUMBER AS L_LINENUMBER,
        SRC:L_QUANTITY AS L_QUANTITY,
        SRC:L_EXTENDEDPRICE AS L_EXTENDEDPRICE,
        SRC:L_DISCOUNT AS L_DISCOUNT,
        SRC:L_TAX AS L_TAX,
        SRC:L_RETURNFLAG AS L_RETURNFLAG,
        SRC:L_LINESTATUS AS L_LINESTATUS,
        SRC:L_SHIPDATE AS L_SHIPDATE,
        SRC:L_COMMITDATE AS L_COMMITDATE,
        SRC:L_RECEIPTDATE AS L_RECEIPTDATE,
        SRC:L_SHIPINSTRUCT AS L_SHIPINSTRUCT,
        SRC:L_SHIPMODE AS L_SHIPMODE,
        SRC:L_COMMENT AS L_COMMENT
    FROM 
        lineitem_std_stream
    WHERE metadata$action='INSERT'
) AS li_stg
ON li.L_ORDERKEY = li_stg.L_ORDERKEY AND li.L_PARTKEY = li_stg.L_PARTKEY AND li.L_SUPPKEY = li_stg.L_SUPPKEY
WHEN matched THEN UPDATE 
SET 
    li.L_PARTKEY = li_stg.L_PARTKEY,
    li.L_SUPPKEY = li_stg.L_SUPPKEY,
    li.L_LINENUMBER = li_stg.L_LINENUMBER,
    li.L_QUANTITY = li_stg.L_QUANTITY,
    li.L_EXTENDEDPRICE = li_stg.L_EXTENDEDPRICE,
    li.L_DISCOUNT = li_stg.L_DISCOUNT,
    li.L_TAX = li_stg.L_TAX,
    li.L_RETURNFLAG = li_stg.L_RETURNFLAG,
    li.L_LINESTATUS = li_stg.L_LINESTATUS,
    li.L_SHIPDATE = li_stg.L_SHIPDATE,
    li.L_COMMITDATE = li_stg.L_COMMITDATE,
    li.L_RECEIPTDATE = li_stg.L_RECEIPTDATE,
    li.L_SHIPINSTRUCT = li_stg.L_SHIPINSTRUCT,
    li.L_SHIPMODE = li_stg.L_SHIPMODE,
    li.L_COMMENT = li_stg.L_COMMENT
WHEN NOT matched THEN INSERT 
(
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT
) 
VALUES 
(
    li_stg.L_ORDERKEY,
    li_stg.L_PARTKEY,
    li_stg.L_SUPPKEY,
    li_stg.L_LINENUMBER,
    li_stg.L_QUANTITY,
    li_stg.L_EXTENDEDPRICE,
    li_stg.L_DISCOUNT,
    li_stg.L_TAX,
    li_stg.L_RETURNFLAG,
    li_stg.L_LINESTATUS,
    li_stg.L_SHIPDATE,
    li_stg.L_COMMITDATE,
    li_stg.L_RECEIPTDATE,
    li_stg.L_SHIPINSTRUCT,
    li_stg.L_SHIPMODE,
    li_stg.L_COMMENT
);

-- Show the created task information.
-- By default the task state is SUSPENDED
SHOW TASKS;

-- Change the task state to STARTED
ALTER TASK lineitem_load_tsk RESUME;

-- Copy into the json raw table from the stage
COPY INTO lineitem_raw_json FROM @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;

-- Get the total of ingested records
SELECT COUNT(1) FROM lineitem_raw_json;

-- Show the STREAM captured data
SELECT * FROM lineitem_std_stream LIMIT 10;

SELECT *
  FROM TABLE(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 100));

ALTER TASK lineitem_load_tsk SUSPEND;

SELECT COUNT(1) FROM ECOMMERCE_DB.ECOMMERCE_DEV.LINEITEM_RAW_JSON;
SELECT COUNT(1) FROM ECOMMERCE_DB.ECOMMERCE_DEV.LINEITEM;

TRUNCATE TABLE ECOMMERCE_DB.ECOMMERCE_DEV.LINEITEM;
USE ROLE accountadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ecommerce_db;

ALTER TASK lineitem_load_tsk RESUME;

-- Remove previous data ingested into Production
TRUNCATE TABLE lineitem;
SELECT COUNT(1) FROM lineitem; 

-- Remove previous data ingested into Staging
TRUNCATE TABLE lineitem_raw_json;
SELECT COUNT(1) FROM lineitem_raw_json; 

-- Create a snowpipe object to ingest automatically into the stage raw table.
CREATE OR REPLACE PIPE lineitem_pipe AUTO_INGEST=true AS
COPY INTO lineitem_raw_json FROM @stg_lineitem_json_dev;

-- Get/copy the SQS queue ARN 
SHOW PIPES;

-- Create an SQS event notification in the main S3 bucket using the copied SQS queue ARN

-- Show the last hour pipe ingestion history 
SELECT *
  FROM TABLE(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour', -1, current_timestamp()),
    pipe_name => 'ecommerce_db.ecommerce_dev.lineitem_pipe'));

-- Get total ingested records into staging
SELECT COUNT(1) FROM lineitem_raw_json; 

-- Get total ingested records into production
SELECT COUNT(1) FROM lineitem; 

ALTER TASK lineitem_load_tsk SUSPEND;
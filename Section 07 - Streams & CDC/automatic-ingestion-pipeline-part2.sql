USE ROLE accountadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ecommerce_db;

--ALTER TASK lineitem_load_tsk RESUME;
-- ALTER TASK lineitem_load_tsk SUSPEND;

-- Remove previous data ingested into Production
TRUNCATE TABLE lineitem;
SELECT COUNT(1) FROM lineitem; 

-- Create a snowpipe object to ingest automatically into the stage raw table.
CREATE OR REPLACE PIPE lineitem_pipe AUTO_INGEST=true AS
COPY INTO lineitem FROM @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;



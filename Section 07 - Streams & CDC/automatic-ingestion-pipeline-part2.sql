USE ROLE accountadmin;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ecommerce_db;

CREATE OR REPLACE TASK lineitem_load_tsk 
-- warehouse = compute_wh
WAREHOUSE = WH_DEV
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
WHEN MATCHED THEN UPDATE
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
WHEN NOT MATCHED THEN INSERT
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


SHOW TASKS;

ALTER TASK lineitem_load_tsk RESUME;

COPY INTO lineitem_raw_json FROM @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;

SELECT *
  FROM TABLE(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 100));

CREATE OR REPLACE PIPE lineitem_pipe AUTO_INGEST=true AS
COPY INTO lineitem FROM @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;



use role sysadmin;
-- use role accountadmin

use schema "ECOMMERCE_DB"."ECOMMERCE_DEV";

-- CSV File Format ----
CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';

---- PARQUET FILE FORMAT ----
CREATE OR REPLACE FILE FORMAT parquet_load_format 
TYPE = 'parquet' ;

---- JSON FILE FORMAT ----
CREATE OR REPLACE FILE FORMAT json_load_format
TYPE = 'JSON'  ;
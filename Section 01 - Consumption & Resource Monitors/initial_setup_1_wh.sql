-- Creates Warehouse

USE ROLE ACCOUNTADMIN; 

DROP WAREHOUSE IF EXISTS COMPUTE_WH;
DROP WAREHOUSE IF EXISTS PROD_XL;

USE ROLE SYSADMIN; 

CREATE WAREHOUSE COMPUTE_WH 
WITH 
WAREHOUSE_SIZE = 'MEDIUM'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 2
SCALING_POLICY = 'ECONOMY';

-- CREATE WAREHOUSE PROD_XL 
-- WITH 
-- WAREHOUSE_SIZE = 'XLARGE'
-- WAREHOUSE_TYPE = 'STANDARD'
-- AUTO_SUSPEND = 300
-- AUTO_RESUME = TRUE
-- MIN_CLUSTER_COUNT = 1
-- MAX_CLUSTER_COUNT = 2
-- SCALING_POLICY = 'ECONOMY';



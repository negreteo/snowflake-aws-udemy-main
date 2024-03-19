USE ROLE accountadmin;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA "ECOMMERCE_DB"."ECOMMERCE_DEV";

CREATE ROLE udf_role;

GRANT USAGE ON warehouse compute_wh TO role udf_role;

GRANT USAGE ON database ECOMMERCE_DB TO role udf_role;

GRANT USAGE ON schema ECOMMERCE_DEV TO role udf_role;

GRANT SELECT ON all tables IN schema ECOMMERCE_DEV TO role udf_role;

-- Granting access to specific function
GRANT all privileges ON function sales_qty_by_supplier(date, number) TO role udf_role;

create or replace user udf_developer password='udf_developer' must_change_password=false;

GRANT role udf_role TO user udf_developer;

ALTER user udf_developer SET default_role = udf_role;

-- Test the GET_DDL using the new user account created above  -----
USE SCHEMA "ECOMMERCE_DB"."ECOMMERCE_DEV";
SELECT get_ddl('function','sales_qty_by_supplier(date,number)');
SELECT sales_qty_by_supplier('1998-05-22', 8939312) AS total_qty_shipped;


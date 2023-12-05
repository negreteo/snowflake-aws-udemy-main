use role accountadmin;
use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

create or replace role view_role;

grant usage on warehouse compute_wh to role view_role;
grant usage on database ECOMMERCE_DB to role view_role;
grant usage on schema ECOMMERCE_LIV to role view_role;

-- Granting select only access
grant select on "ECOMMERCE_DB"."ECOMMERCE_LIV"."SECURE_VW_AGGREGATED_ORDERS"  to role view_role;
grant select on "ECOMMERCE_DB"."ECOMMERCE_LIV"."URGENT_PRIORITY_ORDERS"  to role view_role;
grant select on "ECOMMERCE_DB"."ECOMMERCE_LIV"."VW_AGGREGATED_ORDERS"  to role view_role;

-- Grant the role to your user
grant role view_role to user negreteo;

-- Test the GET_DDL using the new role created above  -----

use role view_role;

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

select * from SECURE_VW_AGGREGATED_ORDERS limit 20;

-- Get access to the underline query of the view
select get_ddl('view','URGENT_PRIORITY_ORDERS');

-- Do not return the underline query of the view because it is secured.
select get_ddl('view','SECURE_VW_AGGREGATED_ORDERS');


-- use role sysadmin ; 
use role accountadmin ; 
use warehouse compute_wh;
use database ecommerce_db;
use schema ecommerce_db.ecommerce_liv;

--- Temporary Table ---
create or replace temporary table orders_tmp as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" limit 50;

--- Transient Table ---
create or replace transient table orders as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" limit 50;

---- Transient Schema ----
create transient schema transient_schema;
use schema transient_schema;
create or replace table orders as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" limit 50;

---- Transient Database ----
create transient database transient_db;
create schema test_schema;
use database transient_db;
use schema transient_db.test_schema;
create or replace table orders as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" limit 50;

--- Convert Permanent table to transient table ----- 
use schema ecommerce_db.ecommerce_liv;
create or replace table permanent_orders as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" limit 50;
create or replace transient table transient_orders as select * from permanent_orders limit 50;

drop table permanent_orders;

ALTER TABLE transient_orders RENAME TO permanent_orders;


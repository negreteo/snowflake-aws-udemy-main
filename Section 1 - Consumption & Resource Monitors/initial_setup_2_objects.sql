-- Creates database, schema and initial tables

USE ROLE SYSADMIN; 
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE DATABASE ecommerce_db;

CREATE SCHEMA IF NOT EXISTS ecommerce_liv;
USE SCHEMA ecommerce_db.ecommerce_liv;

CREATE OR REPLACE TABLE LINEITEM cluster by (L_SHIPDATE) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" limit 2000000;
CREATE OR REPLACE TABLE ORDERS as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS" ;

CREATE SCHEMA IF NOT EXISTS ecommerce_dev;
USE SCHEMA ecommerce_db.ECOMMERCE_DEV;

CREATE OR REPLACE TABLE LINEITEM cluster by (L_SHIPDATE) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" limit 2000000;
CREATE OR REPLACE TABLE ORDERS as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS" ;
use role sysadmin;

-- USE ROLE IAM_SFLK_D_BI
-- USE WAREHOUSE WH_DEV

use database ecommerce_db;
 
create or replace schema streams_test;

--- Create a raw table to test the streams  ---
CREATE OR REPLACE TABLE members_raw (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL
);

--- Create a production table which will consume the streams data  ---
CREATE OR REPLACE TABLE members_prod (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL
);


--- Create a append-only stream on the raw table :members_raw---
CREATE OR REPLACE STREAM members_append_stream ON TABLE members_raw append_only=true;

--- Check the streams (should be empty) ---- 
select * from members_append_stream;

--- Check the stream offset (should be 0) ---- 
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_append_stream') as members_table_st_offset;

SELECT to_timestamp(SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_append_stream')) as members_table_st_offset;

--- Insert some data into the raw table : members_raw --- 
INSERT INTO members_raw (id,name,fee)
VALUES
(1,'Joe',0),
(2,'Jane',0),
(3,'George',0),
(4,'Betty',0),
(5,'Sally',0);

--- Check the streams ---- 
select * from members_append_stream;

--- Check the stream offset  ---- 
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_append_stream') as members_table_st_offset;

--- Query the streams data by ingesting the CDC streams data into the production table ---- 
SELECT id, name, fee FROM members_append_stream;

--- Consume the streams data by ingesting the CDC streams data into the production table : DML Operation ---- 
INSERT INTO members_prod(id,name,fee) 
SELECT id, name, fee FROM members_append_stream;

--- Check the production table -----
select * from members_prod;

--- Check the streams -----
select * from members_append_stream;

--- Check the offset ---- 
SELECT to_timestamp(SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_append_stream')) as members_table_st_offset;

--- Update the raw table (SHOULD NOT work because it is only an APPEND stream) ----
update members_raw set fee=10 where id=4;

--- Check the streams (It does NOT capture the UPDATE because it is an APPEND stream) -----
select * from members_append_stream;

--- Delete the record from the raw table
delete from members_raw where id=4;

--- Check the streams (It does NOT capture the UPDATE because it is an APPEND stream) -----
select * from members_append_stream;

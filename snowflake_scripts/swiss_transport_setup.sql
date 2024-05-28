-- new user creation for all further operations
use role accountadmin;
create user user_01
    password = 'Snowp4rk'
    default_role = sysadmin
    default_secondary_roles = ('ALL')
    must_change_password = false
    comment = 'a user for snowpark project'

-- warehouse creation 
create warehouse transport_wh
    with 
    warehouse_size = 'small' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true;
    
grant role sysadmin to user user_01;
grant usage on warehouse transport_wh to role sysadmin;

use role sysadmin;
use warehouse transport_wh;

-- database & schemas creation
create database if not exists swiss_transport;
use database swiss_transport;

create schema if not exists raw;
create schema if not exists curated;
create schema if not exists consumption;
create schema if not exists common;

-- file formats creation
use schema common;
create or replace file format my_csv_format
    type = 'csv'
    compression = auto
    skip_header=1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    field_delimiter = ';'
    record_delimiter = '\n'
    comment = 'File format for .csv files';

create or replace file format my_json_format
  type = json
  strip_outer_array = true
  compression = auto
  comment = 'File format for .json files';

-- stage creation for files ingestion
use schema raw;
create or replace stage my_stg;

-- sequences for identifying rows
create or replace sequence transport_seq
    start = 1 
    increment = 1 
    comment='This is sequence for raw_transport table';


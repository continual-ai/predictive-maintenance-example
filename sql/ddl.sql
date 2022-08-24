begin;

set database_name = 'PREDICTIVE_MAINTENANCE';
set schema_name = 'VEHICLE_DATA';
set role_name = '<insert_role>';

-- create database & schema
create database if not exists identifier($database_name);

-- grant access
grant CREATE SCHEMA, MONITOR, USAGE
on database identifier($database_name)
to role identifier($role_name);

use database identifier($database_name);

create schema if not exists identifier($schema_name);

grant all privileges
on schema identifier($schema_name)
to role identifier($role_name);

create or replace TABLE USAGE_DATA (
	ASSET VARCHAR(16777216),
    TIME NUMBER(38,0),
    USE FLOAT
);

create or replace TABLE MAINTENANCE_RECORDS (
	ASSET VARCHAR(16777216),
    TIME NUMBER(38,0),
    REASON VARCHAR(16777216),
    PART VARCHAR(16777216),
    QUANTITY NUMBER(38,0)
);

create or replace TABLE FAILURES (
    ASSET VARCHAR(16777216),
    failure_bin NUMBER(38,0)
);

commit;
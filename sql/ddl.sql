begin;

set database_name = 'PREDICTIVE_MAINTENANCE';
set schema_name = 'AZURE_VM';
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

use schema identifier($schema_name);

create or replace TABLE ERRORS (
    DATETIME TIMESTAMP_NTZ, 
    MACHINEID NUMBER(38,0),
    ERRORID VARCHAR(16777216)
);

create or replace TABLE FAILURES (
    DATETIME TIMESTAMP_NTZ, 
    MACHINEID NUMBER(38,0),
    FAILURE VARCHAR(16777216)
);

create or replace TABLE MACHINES (
	MACHINEID NUMBER(38,0),
    MODEL VARCHAR(16777216),
    AGE NUMBER(38,0)
);

create or replace TABLE MAINTENANCE (
	DATETIME TIMESTAMP_NTZ,
    MACHINEID NUMBER(38,0),
    COMP VARCHAR(16777216)
);

create or replace TABLE TELEMETRY (
    DATETIME TIMESTAMP_NTZ,
    MACHINEID NUMBER(38,0),
    VOLT NUMBER(38,0),
    ROTATE NUMBER(38,0),
    PRESSURE NUMBER(38,0),
    VIBRATION NUMBER(38,0)
);

commit;
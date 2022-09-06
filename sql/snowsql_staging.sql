USE database PREDICTIVE_MAINTENANCE;

CREATE STAGE IF NOT EXISTS predictive_maintenance;

PUT file:///path/to/file/PdM_errors.csv @predictive_maintenance;
PUT file:///path/to/file/PdM_failures.csv @predictive_maintenance;
PUT file:///path/to/file/PdM_machines.csv @predictive_maintenance;
PUT file:///path/to/file/PdM_maint.csv @predictive_maintenance;
PUT file:///path/to/file/PdM_telemetry.csv @predictive_maintenance;

COPY INTO PREDICTIVE_MAINTENANCE.AZURE_VM.ERRORS
FROM @predictive_maintenance files = ('PdM_errors.csv.gz')
file_format = (type = CSV skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO PREDICTIVE_MAINTENANCE.AZURE_VM.FAILURES
FROM @predictive_maintenance files = ('PdM_failures.csv.gz')
file_format = (type = CSV skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO PREDICTIVE_MAINTENANCE.AZURE_VM.MACHINES
FROM @predictive_maintenance files = ('PdM_machines.csv.gz')
file_format = (type = CSV skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO PREDICTIVE_MAINTENANCE.AZURE_VM.MAINTENANCE
FROM @predictive_maintenance files = ('PdM_maint.csv.gz')
file_format = (type = CSV skip_header = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO PREDICTIVE_MAINTENANCE.AZURE_VM.TELEMETRY
FROM @predictive_maintenance files = ('PdM_telemetry.csv.gz')
file_format = (type = CSV skip_header = 1);

SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.USAGE_DATA LIMIT 10;
SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.MAINTENANCE_RECORDS LIMIT 10;
SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.FAILURES LIMIT 10;
SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.FAILURES LIMIT 10;
SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.FAILURES LIMIT 10;

DROP STAGE IF EXISTS predictive_maintenance;
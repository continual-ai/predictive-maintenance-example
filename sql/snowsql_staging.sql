USE database PREDICTIVE_MAINTENANCE;

CREATE STAGE IF NOT EXISTS predictive_maintenance;

PUT file:///path/to/file/usage_data.csv @predictive_maintenance;
PUT file:///path/to/file/maintenance_part_consumption.csv @predictive_maintenance;
PUT file:///path/to/file/maintenance_failure.csv @predictive_maintenance;

COPY INTO PREDICTIVE_MAINTENANCE.VEHICLE_DATA.USAGE_DATA
FROM @predictive_maintenance files = ('usage_data.csv.gz')
file_format = (type = CSV skip_header = 1);

COPY INTO PREDICTIVE_MAINTENANCE.VEHICLE_DATA.MAINTENANCE_RECORDS
FROM @predictive_maintenance files = ('maintenance_part_consumption.csv.gz')
file_format = (type = CSV skip_header = 1);

COPY INTO PREDICTIVE_MAINTENANCE.VEHICLE_DATA.FAILURES
FROM @predictive_maintenance files = ('maintenance_failure.csv.gz')
file_format = (type = CSV skip_header = 1);

SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.USAGE_DATA LIMIT 10;
SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.MAINTENANCE_RECORDS LIMIT 10;
SELECT * FROM PREDICTIVE_MAINTENANCE.VEHICLE_DATA.FAILURES LIMIT 10;

DROP STAGE IF EXISTS predictive_maintenance;
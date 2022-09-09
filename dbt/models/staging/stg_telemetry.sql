select
  cast(datetime as timestamp) as ts,
  machineid as machine_id,
  volt as voltage,
  rotate as rotation,
  pressure,
  vibration

from {{ source('Azure_VM', 'telemetry') }}
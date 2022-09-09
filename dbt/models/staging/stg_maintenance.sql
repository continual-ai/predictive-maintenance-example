select
  cast(datetime as timestamp) as ts,
  machineid as machine_id,
  comp

from {{ source('Azure_VM', 'maintenance') }}
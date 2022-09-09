select
  cast(datetime as timestamp) as ts,
  machineid as machine_id,
  failure

from {{ source('Azure_VM', 'failures') }}
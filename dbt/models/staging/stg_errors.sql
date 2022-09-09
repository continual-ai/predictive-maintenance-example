select
  cast(datetime as timestamp) as ts,
  machineid as machine_id,
  errorid as error_id

from {{ source('Azure_VM', 'errors') }}
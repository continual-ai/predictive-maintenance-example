select
  machineid as machine_id,
  model,
  age

from {{ source('Azure_VM', 'machines') }}
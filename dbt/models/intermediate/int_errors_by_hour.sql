{{
  config(
    materialized = 'table'
  )
}}

with telemetry as (

    select * from {{ ref('vm_telemetry') }}

),

errors as (

    select * from {{ ref('vm_errors') }}

),

joined as (

    select
      telemetry.machine_id,
      telemetry.ts,
      errors.error1,
      errors.error2,
      errors.error3,
      errors.error4,
      errors.error5

    from telemetry
    left join errors
    using (machine_id, ts)
)

select * from joined
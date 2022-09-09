{{
  config(
    materialized = 'table'
  )
}}

with maintenance as (

    select * from {{ ref('vm_maintenance') }}

),

failures as (

    select * from {{ ref('vm_failures') }}

),

errors as (

    select * from {{ ref('vm_errors') }}

),

telemetry as (

    select * from {{ ref('vm_telemetry') }}

),

joined as (

    select
      machine_id,
      ts,

      ifnull((select max(ts) from maintenance where machine_id = telemetry.machine_id and ts < telemetry.ts), '2015-01-01 00:00:00.000') as most_recent_maint,
      ifnull((select max(ts) from maintenance where machine_id = telemetry.machine_id and ts < telemetry.ts and comp1_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp1_maint,
      ifnull((select max(ts) from maintenance where machine_id = telemetry.machine_id and ts < telemetry.ts and comp2_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp2_maint,
      ifnull((select max(ts) from maintenance where machine_id = telemetry.machine_id and ts < telemetry.ts and comp3_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp3_maint,
      ifnull((select max(ts) from maintenance where machine_id = telemetry.machine_id and ts < telemetry.ts and comp4_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp4_maint,

      ifnull((select max(ts) from failures where machine_id = telemetry.machine_id and ts < telemetry.ts), '2015-01-01 00:00:00.000') as most_recent_failure,
      ifnull((select max(ts) from failures where machine_id = telemetry.machine_id and ts < telemetry.ts and comp1_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp1_failure,
      ifnull((select max(ts) from failures where machine_id = telemetry.machine_id and ts < telemetry.ts and comp2_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp2_failure,
      ifnull((select max(ts) from failures where machine_id = telemetry.machine_id and ts < telemetry.ts and comp3_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp3_failure,
      ifnull((select max(ts) from failures where machine_id = telemetry.machine_id and ts < telemetry.ts and comp4_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp4_failure,

      ifnull((select max(ts) from errors where machine_id = telemetry.machine_id and ts < telemetry.ts), '2015-01-01 00:00:00.000') as most_recent_error,
      ifnull((select max(ts) from errors where machine_id = telemetry.machine_id and ts < telemetry.ts and error1 > 0), '2015-01-01 00:00:00.000') as most_recent_error1,
      ifnull((select max(ts) from errors where machine_id = telemetry.machine_id and ts < telemetry.ts and error2 > 0), '2015-01-01 00:00:00.000') as most_recent_error2,
      ifnull((select max(ts) from errors where machine_id = telemetry.machine_id and ts < telemetry.ts and error3 > 0), '2015-01-01 00:00:00.000') as most_recent_error3,
      ifnull((select max(ts) from errors where machine_id = telemetry.machine_id and ts < telemetry.ts and error4 > 0), '2015-01-01 00:00:00.000') as most_recent_error4,
      ifnull((select max(ts) from errors where machine_id = telemetry.machine_id and ts < telemetry.ts and error5 > 0), '2015-01-01 00:00:00.000') as most_recent_error5

    from telemetry

)

select * from joined
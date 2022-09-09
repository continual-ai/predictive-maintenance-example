{{
  config(
    materialized = 'table'
  )
}}

with failures as (

    select * from {{ ref('vm_failures') }}

),

telemetry as (

    select * from {{ ref('vm_telemetry') }}

),

joined as (

  select 
    machine_id,
    ts,

    (select min(ts) from failures where machine_id = telemetry.machine_id and ts >= telemetry.ts) as next_failure,
    (select min(ts) from failures where machine_id = telemetry.machine_id and ts >= telemetry.ts and comp1_failure > 0) as next_comp1_failure,
    (select min(ts) from failures where machine_id = telemetry.machine_id and ts >= telemetry.ts and comp2_failure > 0) as next_comp2_failure,
    (select min(ts) from failures where machine_id = telemetry.machine_id and ts >= telemetry.ts and comp3_failure > 0) as next_comp3_failure,
    (select min(ts) from failures where machine_id = telemetry.machine_id and ts >= telemetry.ts and comp4_failure > 0) as next_comp4_failure

    from telemetry

),

final as (

  select
    machine_id,
    ts,
    datediff(hour, ts, next_failure) as hours_until_next_failure,
    datediff(hour, ts, next_comp1_failure) as hours_until_next_comp1_failure,
    datediff(hour, ts, next_comp2_failure) as hours_until_next_comp2_failure,
    datediff(hour, ts, next_comp3_failure) as hours_until_next_comp3_failure,
    datediff(hour, ts, next_comp4_failure) as hours_until_next_comp4_failure

    from joined

)

select * from final
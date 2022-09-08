begin;

create or replace table predictive_maintenance.azure_vm.vm_next_failure_base as (
    with next_failure as (
        select
          machine_id,
          ts,
          (select min(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts >=t1.ts) as next_failure,
          (select min(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts >=t1.ts and comp1_failure > 0) as next_comp1_failure,
          (select min(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts >=t1.ts and comp2_failure > 0) as next_comp2_failure,
          (select min(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts >=t1.ts and comp3_failure > 0) as next_comp3_failure,
          (select min(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts >=t1.ts and comp4_failure > 0) as next_comp4_failure
        from predictive_maintenance.azure_vm.vm_telemetry as t1
    ),
    next_failure_hours as (
        select
          machine_id,
          ts,
          datediff(hour, ts, next_failure) as hours_until_next_failure,
          datediff(hour, ts, next_comp1_failure) as hours_until_next_comp1_failure,
          datediff(hour, ts, next_comp2_failure) as hours_until_next_comp2_failure,
          datediff(hour, ts, next_comp3_failure) as hours_until_next_comp3_failure,
          datediff(hour, ts, next_comp4_failure) as hours_until_next_comp4_failure
        from next_failure
    )
    select
      machine_id,
      ts,
      case when hours_until_next_failure <= 24 then true else false end as failure_in_1day,
      case when hours_until_next_comp1_failure <= 24 then true else false end as comp1_failure_in_1day,
      case when hours_until_next_comp2_failure <= 24 then true else false end as comp2_failure_in_1day,
      case when hours_until_next_comp3_failure <= 24 then true else false end as comp3_failure_in_1day,
      case when hours_until_next_comp4_failure <= 24 then true else false end as comp4_failure_in_1day,
      case when hours_until_next_failure <= (24*7) then true else false end as failure_in_7day,
      case when hours_until_next_comp1_failure <= (24*7) then true else false end as comp1_failure_in_7day,
      case when hours_until_next_comp2_failure <= (24*7) then true else false end as comp2_failure_in_7day,
      case when hours_until_next_comp3_failure <= (24*7) then true else false end as comp3_failure_in_7day,
      case when hours_until_next_comp4_failure <= (24*7) then true else false end as comp4_failure_in_7day,
      case when hours_until_next_failure <= (24*30) then true else false end as failure_in_30day,
      case when hours_until_next_comp1_failure <= (24*30) then true else false end as comp1_failure_in_30day,
      case when hours_until_next_comp2_failure <= (24*30) then true else false end as comp2_failure_in_30day,
      case when hours_until_next_comp3_failure <= (24*30) then true else false end as comp3_failure_in_30day,
      case when hours_until_next_comp4_failure <= (24*30) then true else false end as comp4_failure_in_30day,
      case 
        when ts < '2015-06-01' then 'TRAIN'
        when ts < '2015-09-01' then 'VALI'
        else 'TEST'
      end as split
    from next_failure_hours
    order by machine_id asc, ts desc
);

commit;
begin;

create or replace view predictive_maintenance.azure_vm.vm_errors as (
    select
      cast(datetime as timestamp) as ts,
      machineid as machine_id,
      "'error1'" as error1,
      "'error2'" as error2,
      "'error3'" as error3,
      "'error4'" as error4,
      "'error5'" as error5
    from predictive_maintenance.azure_vm.errors 
    pivot (count(errorid) for errorid in ('error1', 'error2','error3','error4','error5')) as p
    order by machine_id asc, ts desc

);

create or replace view predictive_maintenance.azure_vm.vm_failures as (
    select
      cast(datetime as timestamp) as ts,
      machineid as machine_id,
      "'comp1'" as comp1_failure,
      "'comp2'" as comp2_failure,
      "'comp3'" as comp3_failure,
      "'comp4'" as comp4_failure
    from predictive_maintenance.azure_vm.failures 
    pivot (count(failure) for failure in ('comp1', 'comp2','comp3','comp4')) as p
);

create or replace view predictive_maintenance.azure_vm.vm_machines as (
    select 
      * 
    from predictive_maintenance.azure_vm.machines
);

create or replace view predictive_maintenance.azure_vm.vm_maintenance as (
    select
      cast(datetime as timestamp) as ts,
      machineid as machine_id,
      "'comp1'" as comp1_maint,
      "'comp2'" as comp2_maint,
      "'comp3'" as comp3_maint,
      "'comp4'" as comp4_maint
    from predictive_maintenance.azure_vm.maintenance 
    pivot (count(comp) for comp in ('comp1', 'comp2','comp3','comp4')) as p
);

create or replace view predictive_maintenance.azure_vm.vm_telemetry as (
    select
      cast(datetime as timestamp) as ts,
      machineid as machine_id,
      volt as voltage,
      rotate as rotation,
      pressure,
      vibration,
      avg(voltage) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as voltage_24hr_avg,
      max(voltage) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as voltage_24hr_max,
      min(voltage) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as voltage_24hr_min,
      voltage - voltage_24hr_avg as voltage_diff_from_24hr_avg,
      avg(rotation) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as rotation_24hr_avg,
      max(rotation) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as rotation_24hr_max,
      min(rotation) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as rotation_24hr_min,
      rotation - rotation_24hr_avg as rotation_diff_from_24hr_avg,
      avg(pressure) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as pressure_24hr_avg,
      max(pressure) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as pressure_24hr_max,
      min(pressure) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as pressure_24hr_min,
      pressure - pressure_24hr_avg as pressure_diff_from_24hr_avg,
      avg(vibration) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as vibration_24hr_avg,
      max(vibration) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as vibration_24hr_max,
      min(vibration) over (partition by machine_id order by ts desc rows between 1 following and 24 following) as vibration_24hr_min,
      vibration - vibration_24hr_avg as vibration_diff_from_24hr_avg,
      avg(voltage) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as voltage_7day_avg,
      max(voltage) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as voltage_7day_max,
      min(voltage) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as voltage_7day_min,
      voltage - voltage_7day_avg as voltage_diff_from_7day_avg,
      avg(rotation) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as rotation_7day_avg,
      max(rotation) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as rotation_7day_max,
      min(rotation) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as rotation_7day_min,
      rotation - rotation_7day_avg as rotation_diff_from_7day_avg,
      avg(pressure) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as pressure_7day_avg,
      max(pressure) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as pressure_7day_max,
      min(pressure) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as pressure_7day_min,
      pressure - pressure_7day_avg as pressure_diff_from_7day_avg,
      avg(vibration) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as vibration_7day_avg,
      max(vibration) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as vibration_7day_max,
      min(vibration) over (partition by machine_id order by ts desc rows between 1 following and 168 following) as vibration_7day_min,
      vibration - vibration_7day_avg as vibration_diff_from_7day_avg,
      avg(voltage) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as voltage_30day_avg,
      max(voltage) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as voltage_30day_max,
      min(voltage) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as voltage_30day_min,
      voltage - voltage_30day_avg as voltage_diff_from_30day_avg,
      avg(rotation) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as rotation_30day_avg,
      max(rotation) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as rotation_30day_max,
      min(rotation) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as rotation_30day_min,
      rotation - rotation_30day_avg as rotation_diff_from_30day_avg,
      avg(pressure) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as pressure_30day_avg,
      max(pressure) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as pressure_30day_max,
      min(pressure) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as pressure_30day_min,
      pressure - pressure_30day_avg as pressure_diff_from_30day_avg,
      avg(vibration) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as vibration_30day_avg,
      max(vibration) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as vibration_30day_max,
      min(vibration) over (partition by machine_id order by ts desc rows between 1 following and 720 following) as vibration_30day_min,
      vibration - vibration_30day_avg as vibration_diff_from_30day_avg
    from predictive_maintenance.azure_vm.telemetry
);

create or replace table predictive_maintenance.azure_vm.vm_hourly_status as (
    with machine_status as (
        select
          machine_id,
          ts,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_maintenance where machine_id=t1.machine_id and ts < t1.ts), '2015-01-01 00:00:00.000') as most_recent_maint,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_maintenance where machine_id=t1.machine_id and ts < t1.ts and comp1_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp1_maint,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_maintenance where machine_id=t1.machine_id and ts < t1.ts and comp2_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp2_maint,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_maintenance where machine_id=t1.machine_id and ts < t1.ts and comp3_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp3_maint,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_maintenance where machine_id=t1.machine_id and ts < t1.ts and comp4_maint > 0), '2015-01-01 00:00:00.000') as most_recent_comp4_maint,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts < t1.ts), '2015-01-01 00:00:00.000') as most_recent_failure,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts < t1.ts and comp1_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp1_failure,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts < t1.ts and comp2_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp2_failure,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts < t1.ts and comp3_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp3_failure,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_failures where machine_id=t1.machine_id and ts < t1.ts and comp4_failure > 0), '2015-01-01 00:00:00.000') as most_recent_comp4_failure,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_errors where machine_id=t1.machine_id and ts < t1.ts), '2015-01-01 00:00:00.000') as most_recent_error,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_errors where machine_id=t1.machine_id and ts < t1.ts and error1 > 0), '2015-01-01 00:00:00.000') as most_recent_error1,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_errors where machine_id=t1.machine_id and ts < t1.ts and error2 > 0), '2015-01-01 00:00:00.000') as most_recent_error2,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_errors where machine_id=t1.machine_id and ts < t1.ts and error3 > 0), '2015-01-01 00:00:00.000') as most_recent_error3,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_errors where machine_id=t1.machine_id and ts < t1.ts and error4 > 0), '2015-01-01 00:00:00.000') as most_recent_error4,
          ifnull((select max(ts) from predictive_maintenance.azure_vm.vm_errors where machine_id=t1.machine_id and ts < t1.ts and error5 > 0), '2015-01-01 00:00:00.000') as most_recent_error5
        from predictive_maintenance.azure_vm.vm_telemetry as t1
    )
    select
      machine_id,
      ts,
      datediff(hour, most_recent_maint, ts) as hours_since_last_maint,
      datediff(hour,most_recent_comp1_maint,ts) as hours_since_last_comp1_maint,
      datediff(hour,most_recent_comp2_maint,ts) as hours_since_last_comp2_maint,
      datediff(hour,most_recent_comp3_maint,ts) as hours_since_last_comp3_maint,
      datediff(hour,most_recent_comp4_maint,ts) as hours_since_last_comp4_maint,
      datediff(hour,most_recent_failure,ts) as hours_since_last_failure,
      datediff(hour,most_recent_comp1_failure,ts) as hours_since_last_comp1_failure,
      datediff(hour,most_recent_comp2_failure,ts) as hours_since_last_comp2_failure,
      datediff(hour,most_recent_comp3_failure,ts) as hours_since_last_comp3_failure,
      datediff(hour,most_recent_comp4_failure,ts) as hours_since_last_comp4_failure,
      datediff(hour,most_recent_error,ts) as hours_since_last_error,
      datediff(hour,most_recent_error1,ts) as hours_since_last_error1,
      datediff(hour,most_recent_error2,ts) as hours_since_last_error2,
      datediff(hour,most_recent_error3,ts) as hours_since_last_error3,
      datediff(hour,most_recent_error4,ts) as hours_since_last_error4,
      datediff(hour,most_recent_error5,ts) as hours_since_last_error5
    from machine_status
    order by machine_id asc, ts desc
);

commit;
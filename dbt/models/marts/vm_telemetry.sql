{{
 config(
   meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_telemetry",
       "entity": "azure_vm",
       "index": "machine_id",
       "time_index": "ts"
     }
   }
 ) 
}}

with telemetry as (

    select * from {{ ref('stg_telemetry') }}

),

time_aggregated as (

    select 
      ts,
      machine_id,
      voltage,
      rotation,
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

    from telemetry

)

select * from time_aggregated
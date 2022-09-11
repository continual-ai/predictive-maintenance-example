{{
  config(
    materialized = 'table',
    meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_error_count",
       "entity": "azure_vm",
       "index": "machine_id",
       "time_index": "ts"
     }
   }
  )
}}

with errors_by_hour as (

    select * from {{ ref('int_errors_by_hour') }}

),

final as (

    select
      machine_id,
      ts,
      ifnull(sum(error1 + error2 + error3 + error4 + error5) over (partition by machine_id order by ts desc rows between 1 following and 24 following), 0) as total_errors_24hr_count,
      ifnull(sum(error1) over (partition by machine_id order by ts desc rows between 1 following and 24 following), 0) as error1_24hr_count,
      ifnull(sum(error2) over (partition by machine_id order by ts desc rows between 1 following and 24 following), 0) as error2_24hr_count,
      ifnull(sum(error3) over (partition by machine_id order by ts desc rows between 1 following and 24 following), 0) as error3_24hr_count,
      ifnull(sum(error4) over (partition by machine_id order by ts desc rows between 1 following and 24 following), 0) as error4_24hr_count,
      ifnull(sum(error5) over (partition by machine_id order by ts desc rows between 1 following and 24 following), 0) as error5_24hr_count,
      ifnull(sum(error1 + error2 + error3 + error4 + error5) over (partition by machine_id order by ts desc rows between 1 following and 168 following), 0) as total_errors_7day_count,
      ifnull(sum(error1) over (partition by machine_id order by ts desc rows between 1 following and 168 following), 0) as error1_7day_count,
      ifnull(sum(error2) over (partition by machine_id order by ts desc rows between 1 following and 168 following), 0) as error2_7day_count,
      ifnull(sum(error3) over (partition by machine_id order by ts desc rows between 1 following and 168 following), 0) as error3_7day_count,
      ifnull(sum(error4) over (partition by machine_id order by ts desc rows between 1 following and 168 following), 0) as error4_7day_count,
      ifnull(sum(error5) over (partition by machine_id order by ts desc rows between 1 following and 168 following), 0) as error5_7day_count

    from errors_by_hour

)

select * from final
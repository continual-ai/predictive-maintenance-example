{{
  config(
    materialized = 'table',
    meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_hourly_status",
       "entity": "azure_vm",
       "index": "machine_id",
       "time_index": "ts"
     }
   }
  )
}}

with machine_status as (

    select * from {{ ref('int_vm_status') }}

),

final as (

    select
      machine_id,
      ts,

      datediff(hour, most_recent_maint, ts) as hours_since_last_maint,
      datediff(hour, most_recent_comp1_maint, ts) as hours_since_last_comp1_maint,
      datediff(hour, most_recent_comp2_maint, ts) as hours_since_last_comp2_maint,
      datediff(hour, most_recent_comp3_maint, ts) as hours_since_last_comp3_maint,
      datediff(hour, most_recent_comp4_maint, ts) as hours_since_last_comp4_maint,

      datediff(hour, most_recent_failure, ts) as hours_since_last_failure,
      datediff(hour, most_recent_comp1_failure, ts) as hours_since_last_comp1_failure,
      datediff(hour, most_recent_comp2_failure, ts) as hours_since_last_comp2_failure,
      datediff(hour, most_recent_comp3_failure, ts) as hours_since_last_comp3_failure,
      datediff(hour, most_recent_comp4_failure, ts) as hours_since_last_comp4_failure,

      datediff(hour, most_recent_error, ts) as hours_since_last_error,
      datediff(hour, most_recent_error1, ts) as hours_since_last_error1,
      datediff(hour, most_recent_error2, ts) as hours_since_last_error2,
      datediff(hour, most_recent_error3, ts) as hours_since_last_error3,
      datediff(hour, most_recent_error4, ts) as hours_since_last_error4,
      datediff(hour, most_recent_error5, ts) as hours_since_last_error5

      from machine_status
      order by machine_id asc, ts desc

)

select * from final